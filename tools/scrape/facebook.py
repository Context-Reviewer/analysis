import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================
# CONFIG
# ============================================================
GROUP_ID = "3970539883001618"
TARGET_AUTHOR = "Sean Roy"
SEARCH_QUERY = "sean roy"

GROUP_SEARCH_URL = f"https://www.facebook.com/groups/{GROUP_ID}/search/?q={quote_plus(SEARCH_QUERY)}"

OUTPUT_DIR = Path("fb_extract_out")
HEADLESS = False

# Harvest behavior (Phase 1)
SCROLL_PAUSE_SEC = 1.4               # give FB time to hydrate
HARVEST_MAX_MINUTES = 25             # hard runtime cap for harvesting
HARVEST_NO_NEW_MINUTES = 6           # stop only after this many minutes with no new links
HARVEST_MAX_NEW_LINKS_PER_RUN = 5000 # safety cap

# Scrape behavior (Phase 2)
POST_NAV_TIMEOUT_MS = 60_000
POST_PAGE_WAIT_SEC = 0.8
MAX_POST_PAGES_TO_SCRAPE = 10_000

# Debug
DEBUG_FIRST_N = 12
MAX_DEBUG_FAILURE_ARTIFACTS = 10

# If we harvested links from Sean’s member page, it’s safe to assume author
ASSUME_AUTHOR_FROM_MEMBER_PAGE = True

# ============================================================
# Helpers
# ============================================================
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_jsonl_keys(path: Path, key: str) -> set[str]:
    out = set()
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            o = json.loads(line)
            v = o.get(key)
            if v:
                out.add(v)
        except Exception:
            pass
    return out


def load_jsonl_map(path: Path, key: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            o = json.loads(line)
            k = o.get(key)
            if k:
                out[k] = o
        except Exception:
            pass
    return out


def click_see_more(page):
    page.evaluate(
        """
        () => {
          const needles = new Set(["See more","See More","Show more","Show More","More"]);
          const btns = Array.from(document.querySelectorAll('div[role="button"], span[role="button"], a[role="button"]'));
          let n = 0;
          for (const b of btns) {
            const t = (b.innerText || "").trim();
            if (!t) continue;
            if (!needles.has(t)) continue;
            try { b.click(); n++; } catch {}
            if (n >= 40) break;
          }
          return n;
        }
        """
    )


def scroll_best_effort(page):
    page.evaluate(
        """
        () => {
          function findScrollable() {
            const divs = Array.from(document.querySelectorAll('div'));
            let best = null;
            let bestScore = 0;
            for (const d of divs) {
              const sh = d.scrollHeight || 0;
              const ch = d.clientHeight || 0;
              if (sh <= ch + 200) continue;
              const style = window.getComputedStyle(d);
              const ov = (style.overflowY || "");
              if (!["auto","scroll"].includes(ov)) continue;
              const score = sh - ch;
              if (score > bestScore) { bestScore = score; best = d; }
            }
            return best;
          }
          const sc = findScrollable();
          if (sc) {
            sc.scrollTop = sc.scrollHeight;
            return { mode:"container", sh: sc.scrollHeight, ch: sc.clientHeight };
          }
          window.scrollTo(0, document.body.scrollHeight);
          return { mode:"window", sh: document.body.scrollHeight };
        }
        """
    )


def make_mbasic_url(url: str) -> str:
    p = urlparse(url)
    host = p.netloc.lower()
    if "facebook.com" not in host:
        return url
    return urlunparse((p.scheme or "https", "mbasic.facebook.com", p.path, p.params, p.query, p.fragment))


def utime_to_iso(utime: str) -> str:
    try:
        sec = int(str(utime).strip())
        dt = datetime.fromtimestamp(sec, tz=timezone.utc).astimezone()
        return dt.isoformat(timespec="seconds")
    except Exception:
        return ""


def dump_failure_artifacts(page, idx: int):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUTPUT_DIR / f"debug_fail_{idx:03d}.png"
    html_path = OUTPUT_DIR / f"debug_fail_{idx:03d}.html"
    try:
        page.screenshot(path=str(png_path), full_page=True)
        print(f"[!] wrote screenshot: {png_path}")
    except Exception as e:
        print(f"[!] screenshot failed: {e}")
    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        print(f"[!] wrote html dump:  {html_path}")
    except Exception as e:
        print(f"[!] html dump failed: {e}")


def clean_mbasic_text(raw: str) -> str:
    """
    mbasic returns a lot of UI glue. We aggressively filter common junk lines.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""

    lines = [l.strip() for l in raw.splitlines()]
    out = []
    stop_markers = {
        "Write a comment", "Write a Comment",
        "View more comments", "View previous comments",
        "More Comments", "Add a comment", "Add a Comment",
    }
    junk_exact = {
        "Like", "Comment", "Share", "More", "Full Story",
        "See translation", "See Translation",
        "React", "Reply", "Send", "Copy link", "Copy Link",
        "Privacy · Terms · Advertising · Ad Choices · Cookies · Meta ©",
    }

    for l in lines:
        if not l:
            continue
        if l in stop_markers:
            break
        if l in junk_exact:
            continue
        # junk-ish short lines
        low = l.casefold()
        if len(l) <= 3:
            continue
        if low in {"home", "groups", "watch", "marketplace"}:
            continue
        if low.startswith("people who reacted"):
            continue
        out.append(l)

    text = "\n".join(out).strip()
    # final collapse of excessive blank space
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# ============================================================
# Phase 1: Resolve Sean member page URL from group search
# ============================================================
def resolve_member_url_from_search(page) -> str | None:
    page.goto(GROUP_SEARCH_URL, wait_until="domcontentloaded", timeout=POST_NAV_TIMEOUT_MS)
    time.sleep(3.0)

    print(f"[*] search url={page.url}")
    try:
        print(f"[*] search title={page.title()!r}")
    except Exception:
        pass

    data = page.evaluate(
        f"""
        () => {{
          const anchors = Array.from(document.querySelectorAll('a[href]'));
          const groupUser = [];
          for (const a of anchors) {{
            const href = a.getAttribute('href') || "";
            if (href.includes('/groups/{GROUP_ID}/user/')) {{
              groupUser.push(href);
            }}
          }}
          let best = null;
          let bestLen = 1e9;
          for (const h of groupUser) {{
            const base = h.split('?',1)[0];
            if (base.length < bestLen) {{ bestLen = base.length; best = base; }}
          }}
          return {{ groupUserCount: groupUser.length, bestGroupUserHref: best }};
        }}
        """
    )

    print(f"[*] search found group user links: {data.get('groupUserCount', 0)}")
    best_href = data.get("bestGroupUserHref")
    if not best_href:
        print("[!] Could not find /groups/<gid>/user/<uid>/ link on search page.")
        return None

    full = urljoin("https://www.facebook.com", best_href)
    if not full.endswith("/"):
        full += "/"
    print(f"[*] resolved member page url: {full}")
    return full


# ============================================================
# Phase 1: Harvest post permalinks from Sean member page
# ============================================================
def harvest_post_links_from_member(page, member_url: str, out_links: Path) -> dict:
    seen = load_jsonl_keys(out_links, "permalink")
    new_total = 0

    started = time.monotonic()
    last_new_time = time.monotonic()
    rounds = 0

    page.goto(member_url, wait_until="domcontentloaded", timeout=POST_NAV_TIMEOUT_MS)
    time.sleep(3.0)

    with out_links.open("a", encoding="utf-8") as f:
        while True:
            rounds += 1
            click_see_more(page)

            hrefs = page.evaluate(
                f"""
                () => {{
                  const out = new Set();
                  const anchors = Array.from(document.querySelectorAll('a[href]'));
                  for (const a of anchors) {{
                    const h = a.getAttribute('href');
                    if (!h) continue;
                    const base = h.split('?', 1)[0];
                    if (base.includes('/groups/{GROUP_ID}/posts/')) out.add(base);
                    if (base.includes('/groups/{GROUP_ID}/permalink/')) out.add(base);
                  }}
                  return Array.from(out);
                }}
                """
            ) or []

            new_round = 0
            for h in hrefs:
                full = urljoin("https://www.facebook.com", h)
                if not full.endswith("/"):
                    full += "/"
                if full in seen:
                    continue
                seen.add(full)

                f.write(json.dumps(
                    {
                        "permalink": full,
                        "captured_at": now_iso(),
                        "source": "member_page",
                        "expected_author": TARGET_AUTHOR,
                    },
                    ensure_ascii=False
                ) + "\n")
                f.flush()
                new_round += 1
                new_total += 1

                if HARVEST_MAX_NEW_LINKS_PER_RUN and new_total >= HARVEST_MAX_NEW_LINKS_PER_RUN:
                    break

            if new_round:
                last_new_time = time.monotonic()

            if rounds % 5 == 0 or new_round:
                arts = page.evaluate("() => document.querySelectorAll('[role=\"article\"]').length") or 0
                mins_no_new = (time.monotonic() - last_new_time) / 60.0
                mins_total = (time.monotonic() - started) / 60.0
                print(
                    f"[*] member round={rounds} articles={arts} new_links={new_round} "
                    f"total_new={new_total} no-new-mins={mins_no_new:.1f} total-mins={mins_total:.1f}"
                )

            # Stop conditions
            if HARVEST_MAX_NEW_LINKS_PER_RUN and new_total >= HARVEST_MAX_NEW_LINKS_PER_RUN:
                print("[*] stop: reached HARVEST_MAX_NEW_LINKS_PER_RUN")
                break

            if (time.monotonic() - started) / 60.0 >= HARVEST_MAX_MINUTES:
                print("[*] stop: reached HARVEST_MAX_MINUTES")
                break

            if (time.monotonic() - last_new_time) / 60.0 >= HARVEST_NO_NEW_MINUTES:
                print("[*] stop: no new links for HARVEST_NO_NEW_MINUTES")
                break

            # Scroll
            scroll_best_effort(page)
            time.sleep(SCROLL_PAUSE_SEC)

    return {
        "new_links": new_total,
        "total_links": len(seen),
        "rounds": rounds,
        "elapsed_minutes": (time.monotonic() - started) / 60.0,
    }


# ============================================================
# Phase 2: mbasic extraction (author + timestamp + cleaned text)
# ============================================================
MBASIC_EXTRACT_JS = r"""
() => {
  const norm = s => (s || "").replace(/\s+/g, " ").trim();

  const bodyText = norm(document.body ? document.body.innerText : "");
  const lower = bodyText.toLowerCase();
  const hints = [];
  if (lower.includes("you must log in") || (lower.includes("log in") && lower.includes("password"))) hints.push("login_wall");
  if (lower.includes("content not found") || lower.includes("page isn't available")) hints.push("content_unavailable");

  const anchors = Array.from(document.querySelectorAll('a[href]'))
    .slice(0, 260)
    .map(a => ({ text: norm(a.innerText), href: a.href || "" }))
    .filter(a => a.text && a.href);

  const abbrs = Array.from(document.querySelectorAll('abbr')).slice(0, 12);
  let ts_text = "";
  let ts_title = "";
  let ts_utime = "";
  for (const ab of abbrs) {
    const t = norm(ab.innerText);
    const title = norm(ab.getAttribute('title'));
    const ut = norm(ab.getAttribute('data-utime'));
    if (!t && !title && !ut) continue;
    ts_text = ts_text || t;
    ts_title = ts_title || title;
    ts_utime = ts_utime || ut;
    if (ts_utime || ts_title) break;
  }

  // Try to pick the biggest meaningful block
  const blocks = Array.from(document.querySelectorAll('p, div'))
    .map(el => norm(el.innerText))
    .filter(t => t && t.length >= 50 && t.length <= 30000);

  let best = "";
  for (const t of blocks) {
    if (t.length > best.length) best = t;
  }

  return { ok:true, anchors, ts_text, ts_title, ts_utime, text: best, hints };
}
"""


def score_author_candidates(cands: list[dict], expected_author: str) -> tuple[str, str, list[dict]]:
    expected_cf = (expected_author or "").casefold()
    bad_text = {
        "like","comment","share","more","full story","see translation","reply","react",
        "home","groups","watch","marketplace"
    }

    def looks_like_name(t: str) -> bool:
        t = t.strip()
        if len(t) < 3 or len(t) > 80:
            return False
        parts = t.split()
        if len(parts) < 2:
            return False
        if not re.match(r"^[A-Za-z]", t):
            return False
        return True

    scored = []
    for i, c in enumerate(cands):
        text = norm_ws(c.get("text", ""))
        href = c.get("href", "") or ""
        if not text or not href:
            continue
        t_cf = text.casefold()
        if t_cf in bad_text:
            continue
        if not looks_like_name(text) and expected_cf not in t_cf:
            continue

        score = 0
        score += max(0, 250 - i)

        h = href.lower()
        if "profile.php" in h:
            score += 120
        if "/people/" in h:
            score += 80
        if re.search(r"facebook\.com/[^/?]+/?$", h):
            score += 60

        if any(x in h for x in ["comment", "reply", "reaction", "share"]):
            score -= 120

        if expected_cf and expected_cf in t_cf:
            score += 300

        if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", text):
            score += 30

        scored.append({"score": score, "text": text, "href": href, "idx": i})

    scored.sort(key=lambda x: x["score"], reverse=True)
    if not scored:
        return "", "", []
    best = scored[0]
    return best["text"], best["href"], scored[:8]


def scrape_one_post_mbasic(page, url: str, expected_author: str) -> dict:
    mbasic_url = make_mbasic_url(url)
    page.goto(mbasic_url, wait_until="domcontentloaded", timeout=POST_NAV_TIMEOUT_MS)
    time.sleep(POST_PAGE_WAIT_SEC)
    click_see_more(page)

    mb = page.evaluate(MBASIC_EXTRACT_JS) or {}
    hints = mb.get("hints") or []

    raw_text = (mb.get("text") or "").strip()
    text = clean_mbasic_text(raw_text)

    ts_text = norm_ws(mb.get("ts_text") or "")
    ts_title = norm_ws(mb.get("ts_title") or "")
    ts_utime = norm_ws(mb.get("ts_utime") or "")
    ts_iso = utime_to_iso(ts_utime) if ts_utime else ""

    anchors = mb.get("anchors") or []
    author, author_href, top = score_author_candidates(anchors, expected_author)

    author_source = "mbasic_scored"
    if not author and ASSUME_AUTHOR_FROM_MEMBER_PAGE and expected_author:
        author = expected_author
        author_source = "assumed_expected_author"

    timestamp = ts_iso or ts_title or ts_text

    return {
        "author": author,
        "author_href": author_href,
        "author_source": author_source,
        "timestamp": timestamp,
        "timestamp_iso": ts_iso,
        "timestamp_title": ts_title,
        "timestamp_relative": ts_text,
        "text": text,
        "hints": hints,
        "extraction_source": "mbasic_html",
        "mbasic_url": mbasic_url,
        "author_candidates_top": top,
    }


def scrape_posts(page, in_links: Path, out_all: Path, out_filtered: Path, out_success: Path) -> dict:
    success = load_jsonl_keys(out_success, "permalink")
    link_meta = load_jsonl_map(in_links, "permalink")
    links = list(link_meta.keys())

    print(f"[*] Phase 2: loaded {len(links)} permalinks. success_already={len(success)}")

    failures_dumped = 0
    attempted = 0
    wrote = 0

    for url in links:
        if url in success:
            continue
        attempted += 1
        if attempted > MAX_POST_PAGES_TO_SCRAPE:
            print("[*] stop: MAX_POST_PAGES_TO_SCRAPE")
            break

        expected_author = (link_meta.get(url, {}).get("expected_author") or "").strip()

        try:
            result = scrape_one_post_mbasic(page, url, expected_author)

            author = norm_ws(result.get("author") or "")
            ts = norm_ws(result.get("timestamp") or "")
            text = (result.get("text") or "").strip()
            hints = result.get("hints") or []
            source = result.get("extraction_source") or "unknown"

            # IMPORTANT: only write non-empty posts to posts_all.jsonl
            if not text:
                if failures_dumped < MAX_DEBUG_FAILURE_ARTIFACTS:
                    failures_dumped += 1
                    print(f"[!] EMPTY after mbasic -> dumping artifacts #{failures_dumped} hints={hints} url={url}")
                    dump_failure_artifacts(page, failures_dumped)
                continue

            rec = {
                "permalink": url,
                "captured_at": now_iso(),
                "author": author,
                "author_href": result.get("author_href") or "",
                "author_source": result.get("author_source") or "",
                "timestamp": ts,
                "timestamp_iso": result.get("timestamp_iso") or "",
                "timestamp_title": result.get("timestamp_title") or "",
                "timestamp_relative": result.get("timestamp_relative") or "",
                "text": text,
                "hints": hints,
                "extraction_source": source,
                "mbasic_url": result.get("mbasic_url") or "",
            }

            append_jsonl(out_all, rec)
            wrote += 1

            # Filter file (usually redundant for Sean-only scope)
            if not TARGET_AUTHOR or (TARGET_AUTHOR.casefold() in author.casefold()):
                append_jsonl(out_filtered, rec)

            append_jsonl(out_success, {"permalink": url, "captured_at": now_iso(), "source": source})
            success.add(url)

            if wrote <= DEBUG_FIRST_N:
                tops = result.get("author_candidates_top") or []
                print(f"[DEBUG] {wrote} source={source} author={author!r} ts={ts!r} text_len={len(text)} hints={hints}")
                if tops:
                    print("        top author candidates:")
                    for c in tops[:5]:
                        print(f"          score={c['score']:>4} text={c['text']!r} href={c['href'][:80]}...")

        except Exception as e:
            print(f"[!] error scraping: {e} | {url}")
            continue

    print(f"[*] Phase 2 done. attempted={attempted} wrote={wrote} success_total={len(success)}")
    return {"attempted": attempted, "wrote": wrote, "success_total": len(success)}


# ============================================================
# Main
# ============================================================
def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    out_links = OUTPUT_DIR / "post_links_sean.jsonl"
    out_all = OUTPUT_DIR / "posts_all.jsonl"
    out_filtered = OUTPUT_DIR / "posts_filtered.jsonl"
    out_success = OUTPUT_DIR / "post_scrape_success.jsonl"
    out_meta = OUTPUT_DIR / "run_meta.json"

    run_meta = {"started_at": now_iso(), "group_search_url": GROUP_SEARCH_URL}

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(OUTPUT_DIR / "playwright_profile"),
            headless=HEADLESS,
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        print("\n[!] Log in if needed in the opened browser, clear any checkpoint/continue prompts.")
        print(f"[!] Group search URL: {GROUP_SEARCH_URL}")

        member_url = resolve_member_url_from_search(page)
        if not member_url:
            print("[!] Could not resolve member page. Stopping.")
            ctx.close()
            return

        print("[*] Phase 1: harvesting permalinks from member page (time-based stop)")
        harvest_stats = harvest_post_links_from_member(page, member_url, out_links)
        run_meta["phase1"] = harvest_stats

        print(f"[*] Phase 1 done: new_links={harvest_stats['new_links']} total_links={harvest_stats['total_links']}")

        if harvest_stats["total_links"] == 0:
            print("[!] Got 0 post permalinks. Visually confirm the member page shows posts.")
            ctx.close()
            return

        print("[*] Phase 2: scraping post permalinks via mbasic")
        scrape_stats = scrape_posts(page, out_links, out_all, out_filtered, out_success)
        run_meta["phase2"] = scrape_stats

        ctx.close()

    run_meta["finished_at"] = now_iso()
    out_meta.write_text(json.dumps(run_meta, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\nDone.")
    print(f"Links:     {out_links}")
    print(f"All posts: {out_all}")
    print(f"Filtered:  {out_filtered}")
    print(f"Success:   {out_success}")
    print(f"Run meta:  {out_meta}")


if __name__ == "__main__":
    main()

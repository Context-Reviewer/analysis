
#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

# ---------------- CONFIG ----------------

COMMENT_ARTICLE_SELECTOR = "div[role='article']"

EXPAND_TEXT_SNIPPETS = [
    "View more replies",
    "View more comments",
    "View previous replies",
    "See more",
    "More replies",
]

TIME_RE = re.compile(
    r"^(just now|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs|d|day|days|w|wk|wks|y|yr|yrs))$",
    re.IGNORECASE,
)

UI_STOP_WORDS = {
    "reply", "replies", "like", "share", "edited", "hide", "see translation",
    "write a reply", "send", "following", "follow", "message",
}

# ---------------- HELPERS ----------------

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def is_time_line(s: str) -> bool:
    return bool(TIME_RE.match(normalize_text(s).lower()))

def is_ui_line(s: str) -> bool:
    s = normalize_text(s).lower()
    if not s:
        return True
    if s in UI_STOP_WORDS:
        return True
    if re.fullmatch(r"\d+", s):
        return True
    return False

def parse_article_inner_text(raw: str) -> Tuple[str, str, str]:
    lines = [normalize_text(x) for x in (raw or "").splitlines()]
    lines = [x for x in lines if x]

    if not lines:
        return ("", "", "")

    author = ""
    author_idx = None
    for i, ln in enumerate(lines[:8]):
        if is_time_line(ln):
            continue
        if ln.lower() in UI_STOP_WORDS:
            continue
        author = ln
        author_idx = i
        break

    if author_idx is None:
        return ("", "", "")

    ts = ""
    for ln in lines[author_idx + 1: author_idx + 10]:
        if is_time_line(ln):
            ts = ln
            break

    body_parts: List[str] = []
    for ln in lines[author_idx + 1:]:
        if is_time_line(ln):
            continue
        if is_ui_line(ln):
            if body_parts:
                break
            else:
                continue
        body_parts.append(ln)

    body = normalize_text(" ".join(body_parts))
    return (author, ts, body)

async def scroll_modal_if_present(page, rounds: int):
    dialog = page.locator("div[role='dialog']").first
    if await dialog.count() == 0:
        return False
    await dialog.hover()
    for _ in range(rounds):
        await page.mouse.wheel(0, 1600)
        await asyncio.sleep(0.4)
    return True

async def expand_all(page, max_rounds: int):
    """Click all 'view more' style expanders and scroll the correct surface.

    Facebook often renders the post thread inside a modal dialog (foreground). In that
    case, normal page scrolling affects the background feed, not the comment thread.
    We therefore scope clicks to the dialog when present and scroll the dialog.
    """
    for _ in range(max_rounds):
        did_click = False

        dialog = page.locator("div[role='dialog']").first
        in_dialog = False
        try:
            in_dialog = (await dialog.count()) > 0
        except Exception:
            in_dialog = False

        scope = dialog if in_dialog else page

        # 1) Click expanders inside the correct scope
        for txt in EXPAND_TEXT_SNIPPETS:
            try:
                buttons = scope.get_by_text(txt, exact=False)
                count = await buttons.count()
                for i in range(count):
                    try:
                        await buttons.nth(i).click(timeout=900)
                        did_click = True
                        await asyncio.sleep(0.25)
                    except Exception:
                        pass
            except Exception:
                pass

        # 2) Scroll the correct surface to trigger lazy-loading
        did_scroll_modal = False
        try:
            did_scroll_modal = await scroll_modal_if_present(page, 2)
        except Exception:
            did_scroll_modal = False

        if not did_scroll_modal:
            # Fallback: scroll the page itself when no modal is present
            try:
                await page.mouse.wheel(0, 1600)
            except Exception:
                pass
            await asyncio.sleep(0.35)

        # Stop once stable: no new expanders clicked this round
        if not did_click:
            break
async def extract_comments(page) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    articles = await page.query_selector_all(COMMENT_ARTICLE_SELECTOR)
    for art in articles:
        try:
            raw = await art.inner_text()
            author, ts, body = parse_article_inner_text(raw)
            if not author or not body:
                continue

            comment_id = ""
            cid_a = await art.query_selector("a[href*='comment_id=']")
            if cid_a:
                href = await cid_a.get_attribute("href") or ""
                m = re.search(r"comment_id=(\d+)", href)
                if m:
                    comment_id = m.group(1)

            out.append({
                "comment_author_name": author,
                "comment_timestamp_raw": ts,
                "comment_id": comment_id,
                "comment_text_raw": body,
            })
        except Exception:
            continue
    return out

# ---------------- MAIN ----------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only-url", required=True, help="Single Facebook post permalink")
    ap.add_argument("--out", default="fb_extract_out/comments_raw.jsonl")
    ap.add_argument("--headless", type=int, default=0)
    ap.add_argument("--max-expand-rounds", type=int, default=60)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            user_data_dir="playwright_fb_profile",
            headless=bool(args.headless),
        )
        page = await ctx.new_page()
        await page.goto(args.only_url, wait_until="domcontentloaded")
        await asyncio.sleep(2)

        await expand_all(page, args.max_expand_rounds)

        comments = await extract_comments(page)

        with open(args.out, "w", encoding="utf-8") as f:
            for c in comments:
                rec = {
                    "schema": "comments-raw-1.1",
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "post_permalink": args.only_url,
                    **c,
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        print(f"[✓] Extracted {len(comments)} comments -> {args.out}")
        await ctx.close()

if __name__ == "__main__":
    asyncio.run(main())

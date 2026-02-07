from __future__ import annotations

import html
import json
from pathlib import Path

ENRICHED = Path("fb_extract_out/sean_context_enriched.jsonl")
OUT_DIR = Path("docs/topics")

# Explicit, auditable category mapping (deterministic)
CATEGORY_TOPICS = {
    "israel": ["israel_palestine", "geopolitics"],
    "race": ["race_ethnicity", "dei_woke"],
    "religion": ["religion_morality"],
}

CATEGORY_TITLES = {
    "israel": "Israel / Palestine",
    "race": "Race & Identity",
    "religion": "Religion",
}

MAX_ITEMS = 200
HOSTILE_THRESHOLD = -0.6  # matches your report threshold


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def esc(s: str) -> str:
    return html.escape(s or "", quote=True)


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        fail(f"Missing enriched file: {path}")
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def topics_set(r: dict) -> set[str]:
    t = r.get("topics")
    if isinstance(t, list):
        return {str(x).strip() for x in t if str(x).strip()}
    if isinstance(t, str):
        return {p.strip() for p in t.split("|") if p.strip()}
    return set()


def primary_topic(r: dict) -> str:
    return (r.get("thread_primary_topic") or r.get("primary_topic") or "").strip()


def sort_key(r: dict) -> tuple[int, int]:
    # timeline_i may not exist; fall back to source_index
    ti = r.get("timeline_i")
    try:
        ti_i = int(ti)
    except Exception:
        ti_i = 10**9
    try:
        si = int(r.get("source_index") or 0)
    except Exception:
        si = 0
    return (ti_i, si)


def fmt_sent(x) -> str:
    if x is None:
        return ""
    try:
        return f"{float(x):.3f}"
    except Exception:
        return ""


def compute_summary(items: list[dict]) -> tuple[str, str]:
    # Returns (avg_sentiment_str, hostile_rate_str)
    sentiments: list[float] = []
    hostile = 0
    for r in items:
        sc = r.get("sentiment_compound")
        try:
            v = float(sc)
        except Exception:
            continue
        sentiments.append(v)
        if v < HOSTILE_THRESHOLD:
            hostile += 1

    if sentiments:
        avg = sum(sentiments) / len(sentiments)
        hostile_rate = (hostile / len(sentiments)) * 100.0
        return (f"{avg:.3f}", f"{hostile_rate:.1f}%")
    return ("", "")


def render_page(cat: str, items: list[dict]) -> str:
    title = CATEGORY_TITLES.get(cat, cat)

    avg_s, hostile_rate = compute_summary(items)

    subtitle_parts = [f"{len(items)} comments"]
    if avg_s:
        subtitle_parts.append(f"Avg sentiment: {avg_s}")
    if hostile_rate:
        subtitle_parts.append(f"Hostile (<{HOSTILE_THRESHOLD}): {hostile_rate}")
    subtitle = " · ".join(subtitle_parts)

    # Table rows (deterministic order)
    rows_html: list[str] = []
    for r in items[:MAX_ITEMS]:
        ti = r.get("timeline_i")
        pt = primary_topic(r)
        sc = fmt_sent(r.get("sentiment_compound"))
        thread = r.get("thread_permalink") or r.get("permalink") or ""
        body = (r.get("body") or "").strip()
        preview = body[:280] + ("…" if len(body) > 280 else "")

        link_html = f'<a href="{esc(thread)}">link</a>' if thread else ""

        rows_html.append(
            "<tr>"
            f"<td>{esc(str(ti) if ti is not None else '')}</td>"
            f"<td>{esc(pt)}</td>"
            f"<td>{esc(sc)}</td>"
            f"<td>{link_html}</td>"
            f"<td>{esc(preview)}</td>"
            "</tr>"
        )

    table_html = (
        "<table>"
        "<thead><tr>"
        "<th>timeline_i</th>"
        "<th>primary topic</th>"
        "<th>sentiment</th>"
        "<th>thread</th>"
        "<th>excerpt</th>"
        "</tr></thead>"
        "<tbody>"
        + "".join(rows_html)
        + "</tbody></table>"
    )

    # IMPORTANT: use the site's real stylesheet: ../assets/style.css
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{esc(title)}</title>
  <link rel="stylesheet" href="../assets/style.css"/>
  <script src="../assets/site.js" defer></script>
</head>
<body>
  <header class="topnav">
    <div class="brand">Context Reviewer</div>
    <nav>
      <a href="../index.html">Home</a>
      <a href="../report.html">Report</a>
      <div class="dropdown">
        <a href="#" class="active">Topics ▾</a>
        <div class="dropdown-content">
          <a href="israel.html">Israel / Palestine</a>
          <a href="race.html">Race & Identity</a>
          <a href="religion.html">Religion</a>
        </div>
      </div>
      <a href="../contradictions.html">Contradictions</a>
    </nav>
  </header>

  <main class="container">
    <h1>{esc(title)}</h1>
    <p class="subtitle">{esc(subtitle)}</p>

    <section class="panel">
      {table_html}
    </section>
  </main>

  <footer class="footer">
    Generated report; patterns only; no inference of intent.
  </footer>
</body>
</html>
"""


def main() -> None:
    rows = load_jsonl(ENRICHED)

    # Comments only (consistent with report)
    comments = [r for r in rows if r.get("joined_item_type") == "comment"]

    # Build per-category buckets
    selected: dict[str, list[dict]] = {}
    for cat, wanted in CATEGORY_TOPICS.items():
        wanted_set = set(wanted)
        bucket: list[dict] = []
        for r in comments:
            pt = primary_topic(r)
            ts = topics_set(r)
            if pt in wanted_set or (ts & wanted_set):
                bucket.append(r)

        bucket.sort(key=sort_key)
        selected[cat] = bucket

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for cat, items in selected.items():
        out = OUT_DIR / f"{cat}.html"
        out.write_text(render_page(cat, items), encoding="utf-8")

    print("[OK] Wrote styled category topic pages:")
    for cat, items in selected.items():
        print(f"  {cat}.html: {len(items)} comments")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import html
from pathlib import Path
from collections import defaultdict

ENRICHED = Path("fb_extract_out/sean_context_enriched.jsonl")
OUT_DIR = Path("docs/topics")

# Deterministic category mapping (explicit, auditable)
CATEGORY_TOPICS = {
    "israel": ["israel_palestine", "geopolitics"],
    "race": ["race_ethnicity", "dei_woke"],
    "religion": ["religion_morality"],
}

MAX_ITEMS = 200

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def esc(s: str) -> str:
    return html.escape(s or "", quote=True)

def load_jsonl(path: Path):
    rows = []
    if not path.exists():
        fail(f"Missing enriched file: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows

def item_topics(r: dict) -> list[str]:
    # topics may be a list or a pipe-separated string depending on your pipeline
    t = r.get("topics")
    if isinstance(t, list):
        return [str(x) for x in t if str(x).strip()]
    if isinstance(t, str):
        parts = [p.strip() for p in t.split("|")]
        return [p for p in parts if p]
    return []

def primary_topic(r: dict) -> str:
    return (r.get("thread_primary_topic") or r.get("primary_topic") or "").strip()

def main() -> None:
    rows = load_jsonl(ENRICHED)

    # Use comments only for these pages (matches your report semantics)
    comments = [r for r in rows if r.get("joined_item_type") == "comment"]

    # Build per-category selection
    selected = {}
    for cat, topics in CATEGORY_TOPICS.items():
        topic_set = set(topics)
        bucket = []
        for r in comments:
            pt = primary_topic(r)
            ts = set(item_topics(r))
            if pt in topic_set or (ts & topic_set):
                bucket.append(r)

        # deterministic order: timeline_i
        bucket.sort(key=lambda x: int(x.get("timeline_i") or 10**9))
        selected[cat] = bucket

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for cat, bucket in selected.items():
        out = OUT_DIR / f"{cat}.html"
        title = {
            "israel": "Israel / Palestine",
            "race": "Race & Identity",
            "religion": "Religion",
        }.get(cat, cat)

        lines = []
        lines.append(f"<h1>{esc(title)}</h1>")
        lines.append(f"<p><b>Matched comments:</b> {len(bucket)}</p>")
        lines.append('<p><a href="../report.html">Back to Executive Summary</a></p>')
        lines.append("<hr/>")
        lines.append("<ol>")

        for r in bucket[:MAX_ITEMS]:
            ti = r.get("timeline_i")
            thread = r.get("thread_permalink") or r.get("permalink") or ""
            txt = (r.get("body") or "").strip()
            preview = txt[:280] + ("…" if len(txt) > 280 else "")
            pt = primary_topic(r)
            lines.append(
                "<li>"
                f"<div><b>timeline_i:</b> {esc(str(ti))} | <b>primary:</b> {esc(pt)}</div>"
                + (f'<div><a href="{esc(thread)}">thread link</a></div>' if thread else "")
                + f"<div style='white-space:pre-wrap'>{esc(preview)}</div>"
                "</li>"
            )
        lines.append("</ol>")

        doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{esc(title)}</title>
  <link rel="stylesheet" href="../styles.css"/>
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
    {''.join(lines)}
  </main>

  <footer class="footer">
    Generated report; patterns only; no inference of intent.
  </footer>
</body>
</html>
"""
        out.write_text(doc, encoding="utf-8")

    print("[OK] Wrote category topic pages:")
    for cat, bucket in selected.items():
        print(f"  {cat}.html: {len(bucket)} comments")

if __name__ == "__main__":
    main()

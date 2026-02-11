#!/usr/bin/env python3
"""
Phase 8C (v1): Static HTML Report

Renders Phase 8A narrative into a deterministic, read-only HTML document.
- No JS logic
- No external assets
- No wall-clock timestamps
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def load_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
    )


def main() -> None:
    root = Path(".").resolve()

    f_narr = root / "phase8/out/phase8_narrative.json"
    out_dir = root / "phase8/out"
    out_dir.mkdir(parents=True, exist_ok=True)

    narr = load_json(f_narr)

    lines = []
    lines.append("<!doctype html>")
    lines.append("<html lang=\"en\">")
    lines.append("<head>")
    lines.append("<meta charset=\"utf-8\">")
    lines.append("<title>Context Reviewer — Narrative Report</title>")
    lines.append("<style>")
    lines.append("""
body {
  font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
  margin: 40px;
  line-height: 1.5;
}
h1, h2, h3 {
  line-height: 1.25;
}
.topic {
  margin-top: 32px;
}
.claim {
  margin-top: 20px;
  padding-left: 12px;
  border-left: 3px solid #ccc;
}
.meta {
  color: #555;
  font-size: 0.9em;
}
blockquote {
  background: #f7f7f7;
  padding: 10px 14px;
  margin: 10px 0;
  border-left: 4px solid #999;
}
ul {
  margin-top: 6px;
}
</style>
    """.strip())
    lines.append("</head>")
    lines.append("<body>")

    lines.append("<h1>Narrative Summary</h1>")
    lines.append("<p class=\"meta\">Evidence-first synthesis. Relative chronology only.</p>")

    ov = narr.get("overview", {})
    lines.append("<ul class=\"meta\">")
    lines.append(f"<li>Topics: {ov.get('topics_count')}</li>")
    lines.append(f"<li>Records analyzed: {ov.get('records_total')}</li>")
    lines.append(f"<li>Threads: {ov.get('threads_total')}</li>")
    lines.append("</ul>")

    for topic in narr.get("topics", []):
        lines.append(f"<div class=\"topic\">")
        lines.append(f"<h2>Topic: {html_escape(str(topic.get('topic_id')))}</h2>")

        for cl in topic.get("claims", []):
            lines.append("<div class=\"claim\">")
            lines.append("<h3>Claim</h3>")
            lines.append(f"<p>{html_escape(cl.get('normalized_text',''))}</p>")

            stats = cl.get("activity_window", {})
            lines.append("<p class=\"meta\">")
            lines.append(
                f"Activity window (ordinal): "
                f"{stats.get('start_ordinal')} → {stats.get('end_ordinal')}"
            )
            lines.append("</p>")

            if cl.get("behavior_links"):
                lines.append("<p class=\"meta\">Linked behavioral context:</p>")
                lines.append("<ul>")
                for bl in cl["behavior_links"]:
                    lines.append(
                        f"<li>{html_escape(bl.get('behavior_key'))} overlaps claim window</li>"
                    )
                lines.append("</ul>")

            quotes = cl.get("evidence_quotes", [])
            if quotes:
                lines.append("<p class=\"meta\">Evidence excerpts:</p>")
                for q in quotes:
                    lines.append(f"<blockquote>{html_escape(q)}</blockquote>")

            lines.append("</div>")

        lines.append("</div>")

    lines.append("</body>")
    lines.append("</html>")

    f_html = out_dir / "phase8_report.html"
    f_html.write_text("\n".join(lines) + "\n", encoding="utf-8")

    manifest = {
        "schema_version": "phase8_report_manifest-1.0",
        "inputs": {
            "phase8_narrative.json": sha256_file(f_narr),
        },
        "outputs": {
            "phase8_report.html": sha256_file(f_html),
        },
        "notes": {
            "deterministic": True,
            "no_js": True,
            "no_wall_clock": True,
        },
    }

    f_manifest = out_dir / "phase8_report_manifest.json"
    f_manifest.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    print("[OK] Phase 8C static report complete")
    print(f"[OK] wrote: {f_html.relative_to(root)}")
    print(f"[OK] wrote: {f_manifest.relative_to(root)}")


if __name__ == "__main__":
    main()

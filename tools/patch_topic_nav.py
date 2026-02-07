from __future__ import annotations

import re
from pathlib import Path

TOPICS = [
    ("israel.html", "Israel / Palestine"),
    ("race.html", "Race & Identity"),
    ("religion.html", "Religion"),
]

NAV_BLOCK_RE = re.compile(r"<nav>\s*.*?\s*</nav>", re.DOTALL)

def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)

def build_nav(active_topic_href: str) -> str:
    # Dropdown: mark the dropdown toggle active, and mark the current topic link active.
    topic_links = []
    for href, label in TOPICS:
        cls = ' class="active"' if href == active_topic_href else ""
        topic_links.append(f'                <a href="{href}"{cls}>{label}</a>')
    topic_links_html = "\n".join(topic_links)

    return f"""\
            <nav>
        <a href="../index.html" class="brand">Context Reviewer</a>
        <a href="../index.html">Home</a>
        <a href="../report.html">Report</a>
        <div class="dropdown">
            <a href="#" class="active">Topics ▾</a>
            <div class="dropdown-content">
{topic_links_html}
            </div>
        </div>
        <a href="../contradictions.html">Contradictions</a>
        <a href="../conclusion.html">Conclusion</a>
        <a href="../signals.html">Signals</a>
    </nav>"""

def patch_one(path: Path) -> None:
    html = path.read_text(encoding="utf-8")

    matches = list(NAV_BLOCK_RE.finditer(html))
    if len(matches) != 1:
        fail(f"{path}: expected exactly 1 <nav>...</nav> block, found {len(matches)}")

    active = path.name  # israel.html / race.html / religion.html
    nav = build_nav(active)

    patched = NAV_BLOCK_RE.sub(nav, html)

    if patched == html:
        print(f"[OK] {path}: nav already canonical")
        return

    path.write_text(patched, encoding="utf-8")
    print(f"[OK] {path}: nav patched")

def main() -> None:
    root = Path("docs/topics")
    if not root.exists():
        fail("missing docs/topics directory")

    targets = [root / href for href, _ in TOPICS]
    missing = [str(p) for p in targets if not p.exists()]
    if missing:
        fail("missing topic page(s): " + ", ".join(missing))

    for p in targets:
        patch_one(p)

    print("[OK] patch_topic_nav complete")

if __name__ == "__main__":
    main()

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple


# Match exactly one nav block, capturing its indentation (leading whitespace at the <nav> line).
NAV_BLOCK_RE = re.compile(r"(?ms)^(?P<indent>[ \t]*)<nav>\s*.*?\s*</nav>\s*$")


def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)


def _indent_block(block: str, indent: str) -> str:
    # Prefix each non-empty line with the original indentation.
    lines = block.splitlines()
    out = []
    for ln in lines:
        out.append((indent + ln) if ln.strip() else ln)
    return "\n".join(out)


def build_nav(active: str, topics_active: bool = False) -> str:
    """
    Returns a nav block starting at column 0 (no leading indentation).
    Caller re-indents to match file context.
    """
    active_map: Dict[str, str] = {
        "home": ' class="active"',
        "report": ' class="active"',
        "contradictions": ' class="active"',
        "conclusion": ' class="active"',
        "signals": ' class="active"',
    }

    def a(which: str) -> str:
        return active_map[which] if active == which else ""

    topics_toggle = ' class="active"' if topics_active else ""

    # Canonical nav: single brand anchor, consistent order, includes Signals.
    return f"""<nav>
<a href="index.html" class="brand">Context Reviewer</a>
<a href="index.html"{a("home")}>Home</a>
<a href="report.html"{a("report")}>Report</a>
<div class="dropdown">
    <a href="#"{topics_toggle}>Topics ▾</a>
    <div class="dropdown-content">
        <a href="topics/israel.html">Israel / Palestine</a>
        <a href="topics/race.html">Race & Identity</a>
        <a href="topics/religion.html">Religion</a>
    </div>
</div>
<a href="contradictions.html"{a("contradictions")}>Contradictions</a>
<a href="conclusion.html"{a("conclusion")}>Conclusion</a>
<a href="signals.html"{a("signals")}>Signals</a>
</nav>"""


def patch_html(html: str, active: str, topics_active: bool = False) -> str:
    # Find the nav block with indentation.
    matches = list(NAV_BLOCK_RE.finditer(html))
    if len(matches) != 1:
        fail(f"expected exactly 1 <nav>...</nav> block, found {len(matches)}")

    m = matches[0]
    indent = m.group("indent")

    nav = build_nav(active=active, topics_active=topics_active)
    nav_indented = _indent_block(nav, indent)

    # Replace exactly the matched block (no extra blank lines inserted).
    patched = html[: m.start()] + nav_indented + html[m.end() :]
    return patched


def patch_file(path: Path, active: str, topics_active: bool = False) -> None:
    html = path.read_text(encoding="utf-8")
    patched = patch_html(html, active=active, topics_active=topics_active)

    if patched == html:
        print(f"[OK] {path}: nav already canonical")
        return

    path.write_text(patched, encoding="utf-8")
    print(f"[OK] {path}: nav patched")


def main() -> None:
    targets: List[Tuple[str, str]] = [
        ("docs/index.html", "home"),
        ("docs/report.html", "report"),
        ("docs/contradictions.html", "contradictions"),
        # If you want global nav normalized here too, uncomment:
        # ("docs/conclusion.html", "conclusion"),
        # ("docs/signals.html", "signals"),
    ]

    for file_path, active in targets:
        p = Path(file_path)
        if not p.exists():
            fail(f"missing file: {file_path}")
        patch_file(p, active=active, topics_active=False)

    print("[OK] patch_global_nav complete")


if __name__ == "__main__":
    main()

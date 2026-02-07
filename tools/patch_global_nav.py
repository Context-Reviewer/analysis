from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List


NAV_TEMPLATE = """\
    <nav>
        <a href="index.html" class="brand">Context Reviewer</a>
        <a href="index.html"{home_active}>Home</a>
        <a href="report.html"{report_active}>Report</a>
        <div class="dropdown">
            <a href="#" class="active">Topics ▾</a>
            <div class="dropdown-content">
                <a href="topics/israel.html">Israel / Palestine</a>
                <a href="topics/race.html">Race & Identity</a>
                <a href="topics/religion.html">Religion</a>
            </div>
        </div>
        <a href="contradictions.html"{contradictions_active}>Contradictions</a>
        <a href="conclusion.html"{conclusion_active}>Conclusion</a>
        <a href="signals.html"{signals_active}>Signals</a>
    </nav>
"""

# For pages where "Topics" is not the active section, remove class="active" from Topics anchor.
NAV_TEMPLATE_TOPICS_INACTIVE = NAV_TEMPLATE.replace('href="#" class="active"', 'href="#"')


def build_nav(active: str, topics_active: bool = False) -> str:
    active_map: Dict[str, str] = {
        "home": " class=\"active\"",
        "report": " class=\"active\"",
        "contradictions": " class=\"active\"",
        "conclusion": " class=\"active\"",
        "signals": " class=\"active\"",
    }

    def a(which: str) -> str:
        return active_map[which] if active == which else ""

    tpl = NAV_TEMPLATE if topics_active else NAV_TEMPLATE_TOPICS_INACTIVE
    return tpl.format(
        home_active=a("home"),
        report_active=a("report"),
        contradictions_active=a("contradictions"),
        conclusion_active=a("conclusion"),
        signals_active=a("signals"),
    )


NAV_BLOCK_RE = re.compile(r"<nav>\s*.*?\s*</nav>", re.DOTALL)


def patch_file(path: Path, active: str, topics_active: bool = False) -> None:
    html = path.read_text(encoding="utf-8")

    matches = list(NAV_BLOCK_RE.finditer(html))
    if len(matches) != 1:
        raise SystemExit(
            f"[FAIL] {path}: expected exactly 1 <nav>...</nav> block, found {len(matches)}"
        )

    nav = build_nav(active=active, topics_active=topics_active)
    patched = NAV_BLOCK_RE.sub(nav, html)

    if patched == html:
        print(f"[OK] {path}: nav already canonical")
        return

    path.write_text(patched, encoding="utf-8")
    print(f"[OK] {path}: nav patched")


def main() -> None:
    # Minimal scope: only patch the static top-level pages.
    targets: List[tuple[str, str]] = [
        ("docs/index.html", "home"),
        ("docs/report.html", "report"),
        ("docs/contradictions.html", "contradictions"),
        # Optional (uncomment once you want full unification):
        # ("docs/conclusion.html", "conclusion"),
        # ("docs/signals.html", "signals"),
    ]

    for file_path, active in targets:
        p = Path(file_path)
        if not p.exists():
            raise SystemExit(f"[FAIL] missing file: {file_path}")
        patch_file(p, active=active, topics_active=False)

    print("[OK] patch_global_nav complete")


if __name__ == "__main__":
    main()

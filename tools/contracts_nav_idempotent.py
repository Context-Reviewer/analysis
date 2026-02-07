from __future__ import annotations

from pathlib import Path

from patch_global_nav import patch_html as patch_global_html
from patch_topic_nav import patch_html as patch_topic_html


def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)


def assert_idempotent(name: str, once: str, twice: str) -> None:
    if once != twice:
        fail(f"{name}: nav patch is NOT idempotent (second pass changed output)")


def main() -> None:
    # Global pages patched by patch_global_nav.py
    global_targets = [
        ("docs/index.html", "home"),
        ("docs/report.html", "report"),
        ("docs/contradictions.html", "contradictions"),
    ]

    for path_str, active in global_targets:
        p = Path(path_str)
        if not p.exists():
            fail(f"missing {path_str}")
        html = p.read_text(encoding="utf-8")
        once = patch_global_html(html, active=active, topics_active=False)
        twice = patch_global_html(once, active=active, topics_active=False)
        assert_idempotent(path_str, once, twice)

    # Topic pages patched by patch_topic_nav.py
    topic_targets = [
        Path("docs/topics/israel.html"),
        Path("docs/topics/race.html"),
        Path("docs/topics/religion.html"),
    ]
    for p in topic_targets:
        if not p.exists():
            fail(f"missing {p}")
        html = p.read_text(encoding="utf-8")
        once = patch_topic_html(html, active_topic_href=p.name)
        twice = patch_topic_html(once, active_topic_href=p.name)
        assert_idempotent(str(p), once, twice)

    print("[OK] nav idempotence contract: passed")


if __name__ == "__main__":
    main()

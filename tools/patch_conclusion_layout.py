from __future__ import annotations

import re
from pathlib import Path

SRC = Path("docs/conclusion.html")

NAV_CANON = """\
            <nav>
        <a href="index.html" class="brand">Context Reviewer</a>
        <a href="index.html">Home</a>
        <a href="report.html">Report</a>
        <div class="dropdown">
            <a href="#">Topics ▾</a>
            <div class="dropdown-content">
                <a href="topics/israel.html">Israel / Palestine</a>
                <a href="topics/race.html">Race & Identity</a>
                <a href="topics/religion.html">Religion</a>
            </div>
        </div>
        <a href="contradictions.html">Contradictions</a>
        <a href="conclusion.html" class="active">Conclusion</a>
        <a href="signals.html">Signals</a>
    </nav>
"""

def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)

def main() -> None:
    if not SRC.exists():
        fail(f"missing {SRC}")

    html = SRC.read_text(encoding="utf-8")

    # Ensure we're patching the expected framework-B page
    if "<header" not in html or "<main class=\"container\">" not in html:
        fail("conclusion.html does not contain expected <header ...> + <main class=\"container\"> structure")

    # Keep everything in <head> exactly as-is
    m_head = re.search(r"(?is)\A(.*?</head>)\s*<body>.*\Z", html)
    if not m_head:
        fail("could not isolate </head> boundary")
    head = m_head.group(1)

    # Extract main inner HTML
    m_main = re.search(r'(?is)<main class="container">\s*(.*?)\s*</main>', html)
    if not m_main:
        fail('could not find <main class="container"> ... </main>')
    main_inner = m_main.group(1).strip()

    # Convert panel sections to box rhythm (canonical site skeleton)
    # - section.panel -> div.box
    # - closing section -> closing div
    main_inner = re.sub(r'(?is)<section\s+class="panel"\s*>', '<div class="box">', main_inner)
    main_inner = re.sub(r'(?is)</section\s*>', '</div>', main_inner)

    # Extract footer (if present)
    m_footer = re.search(r"(?is)(<footer\b.*?</footer>)", html)
    footer = m_footer.group(1).strip() if m_footer else ""

    # Preserve <script> blocks unchanged (if any)
    scripts = re.findall(r"(?is)<script\b.*?>.*?</script>", html)
    scripts_blob = "\n\n".join(s.strip() for s in scripts)

    body = f"""\
<body>

{NAV_CANON}

    <div class="container">
{main_inner}
    </div>

{footer}

{scripts_blob}
</body>
</html>
"""

    patched = head + "\n" + body
    SRC.write_text(patched, encoding="utf-8")
    print("[OK] patched docs/conclusion.html to canonical skeleton + canonical nav")

if __name__ == "__main__":
    main()

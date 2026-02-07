from __future__ import annotations

import re
from pathlib import Path

SRC = Path("docs/signals.html")

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
        <a href="conclusion.html">Conclusion</a>
        <a href="signals.html" class="active">Signals</a>
    </nav>
"""

def fail(msg: str) -> None:
    raise SystemExit("[FAIL] " + msg)

def main() -> None:
    if not SRC.exists():
        fail(f"missing {SRC}")

    html = SRC.read_text(encoding="utf-8")

    # Sanity: ensure we're patching the expected framework-B page
    if "<header class=\"topnav\">" not in html or "<main class=\"container\">" not in html:
        fail("signals.html does not contain expected <header class=\"topnav\"> + <main class=\"container\"> structure")

    # Keep everything in <head> exactly as-is (including inline <style>)
    m_head = re.search(r"(?is)\A(.*?</head>)\s*<body>.*\Z", html)
    if not m_head:
        fail("could not isolate </head> boundary")

    head = m_head.group(1)

    # Extract the intro paragraph text from the first panel (keep wording)
    m_intro = re.search(r'(?is)<h1>\s*Signals\s*</h1>\s*(<p[^>]*>.*?</p>)', html)
    if not m_intro:
        fail("could not find Signals intro paragraph (<h1>Signals</h1> ... <p>...)")
    intro_p = m_intro.group(1).strip()

    # Extract status + signals mount points (must exist)
    if 'id="status"' not in html or 'id="signals"' not in html:
        fail('missing required mount points: id="status" and/or id="signals"')
    # Rebuild a minimal, canonical content block using those same ids/classes.
    # (We intentionally do not attempt to preserve the old <section class="panel"> wrappers.)
    status_div = '<div id="status" class="subtle mono">Loading signals_index.json…</div>'
    signals_div = '<div id="signals"></div>'

    # Extract the entire <script>...</script> block(s) from the original (keep JS logic unchanged)
    scripts = re.findall(r"(?is)<script\b.*?>.*?</script>", html)
    if not scripts:
        fail("no <script> blocks found to preserve")
    scripts_blob = "\n\n".join(s.strip() for s in scripts)

    # Build canonical body:
    # - nav directly under body (Framework A)
    # - single .container wrapper
    # - use intro-box + box rhythm consistent with other pages
    body = f"""\
<body>

{NAV_CANON}

    <div class="container">
        <h1>Signals</h1>

        <div class="intro-box">
            {intro_p}
        </div>

        <div class="box">
            {status_div}
            {signals_div}
        </div>

        <div class="box">
            <h3>Reproducibility</h3>
            <p><strong>Index source:</strong> <span class="mono"><a href="data/signals_index.json">docs/data/signals_index.json</a></span></p>
            <p>Rates are normalized per 100 analyzed items as reported by each signal output.</p>
        </div>
    </div>

{scripts_blob}
</body>
</html>
"""

    # Preserve the original doctype + html open tag if present in head chunk;
    # head already includes them, since we captured from start through </head>.
    patched = head + "\n" + body

    SRC.write_text(patched, encoding="utf-8")
    print("[OK] patched docs/signals.html to canonical skeleton + canonical nav")

if __name__ == "__main__":
    main()

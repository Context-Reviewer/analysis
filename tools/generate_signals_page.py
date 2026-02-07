import json
from pathlib import Path

DOCS_DIR = Path("docs")
DATA_DIR = DOCS_DIR / "data"
INDEX_PATH = DATA_DIR / "signals_index.json"
OUT_HTML = DOCS_DIR / "signals.html"

def load_index() -> dict:
    if not INDEX_PATH.exists():
        raise SystemExit(f"Missing: {INDEX_PATH} (run copy step to docs/data)")
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))

def html_template() -> str:
    # Uses existing site primitives: topnav/container/panel + docs/assets/style.css
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Signals</title>
  <link rel="stylesheet" href="assets/style.css" />
  <style>
    /* Minimal additions; keep site CSS as source of truth */
    .signals-grid { display: grid; gap: 16px; }
    .signals-table { width: 100%; border-collapse: collapse; }
    .signals-table th, .signals-table td { padding: 10px 8px; border-bottom: 1px solid rgba(255,255,255,0.08); vertical-align: top; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 0.95em; }
    .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.18); }
    .subtle { opacity: 0.85; }
    .right { text-align:right; }
    .small { font-size: 0.95em; }
    details { border: 1px solid rgba(255,255,255,0.12); border-radius: 10px; padding: 10px 12px; }
    summary { cursor: pointer; }
    .kvs { display:grid; grid-template-columns: 160px 1fr; gap: 6px 12px; }
  </style>
</head>
<body>
  <header class="topnav">
    <div class="container">
      <nav>
        <a href="index.html">Home</a>
        <a href="report.html">Report</a>
        <a href="contradictions.html">Contradictions</a>
        <a href="conclusion.html">Conclusion</a>
        <a class="active" href="signals.html">Signals</a>
      </nav>
    </div>
  </header>

  <main class="container">
    <section class="panel">
      <h1>Signals</h1>
      <p class="subtle">
        Deterministic, dataset-bounded signal outputs. These are evidence-first indicators computed from collected text.
        No intent inference. No labeling. Examples link to published JSON.
      </p>

      <div id="status" class="subtle mono">Loading signals_index.json…</div>
      <div id="signals"></div>
    </section>

    <section class="panel">
      <h2>Reproducibility</h2>
      <div class="kvs small">
        <div class="subtle">Index source</div><div class="mono"><a href="data/signals_index.json">docs/data/signals_index.json</a></div>
        <div class="subtle">Notes</div><div>Rates are normalized per 100 analyzed items as reported by each signal output.</div>
      </div>
    </section>
  </main>

<script>
function pickMetric(metrics, suffixes) {
  const keys = Object.keys(metrics || {});
  for (const suf of suffixes) {
    const k = keys.find(x => x.endsWith(suf));
    if (k) return { key: k, value: metrics[k] };
  }
  return null;
}

function fmtRate(v) {
  if (typeof v === "number") return v.toFixed(4);
  return String(v ?? "");
}

async function main() {
  const status = document.getElementById("status");
  const root = document.getElementById("signals");

  let idx;
  try {
    const res = await fetch("data/signals_index.json", { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    idx = await res.json();
  } catch (e) {
    status.textContent = "Failed to load signals_index.json: " + e;
    return;
  }

  const signals = idx.signals || [];
  status.textContent = "Loaded " + signals.length + " signal(s).";

  // Table
  const table = document.createElement("table");
  table.className = "signals-table";

  const thead = document.createElement("thead");
  thead.innerHTML = `
    <tr>
      <th>Signal</th>
      <th>Tier</th>
      <th class="right">Count</th>
      <th class="right">Rate/100</th>
      <th>Over time</th>
      <th class="right">Examples</th>
      <th>JSON</th>
    </tr>`;
  table.appendChild(thead);

  const tbody = document.createElement("tbody");

  for (const s of signals) {
    const metrics = s.metrics || {};
    const count = pickMetric(metrics, ["_count"])?.value ?? "";
    const rate = pickMetric(metrics, ["_rate_per_100", "_rate"])?.value ?? "";
    const over = pickMetric(metrics, ["_over_time"])?.value ?? {};
    const overTxt = (over && typeof over === "object") ? Object.entries(over).map(([k,v]) => `${k}: ${v}`).join(", ") : "";

    const jsonName = (s.signal_id || "signal") + ".json";
    const jsonHref = "data/" + jsonName;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${s.signal_id ?? ""} <span class="subtle">v${s.signal_version ?? ""}</span></td>
      <td><span class="pill mono">${s.tier ?? ""}</span></td>
      <td class="right mono">${count}</td>
      <td class="right mono">${fmtRate(rate)}</td>
      <td class="small">${overTxt}</td>
      <td class="right mono">${s.examples_count ?? ""}</td>
      <td class="mono"><a href="${jsonHref}">${jsonName}</a></td>
    `;
    tbody.appendChild(tr);

    // Expandable details row (fingerprints + scope)
    const dtr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;

    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.innerHTML = `<span class="mono">Details</span> <span class="subtle">(scope + fingerprints)</span>`;
    details.appendChild(summary);

    const scope = s.dataset_scope || {};
    const fps = s.fingerprints || {};

    const box = document.createElement("div");
    box.className = "kvs small";
    box.style.marginTop = "10px";
    box.innerHTML = `
      <div class="subtle">Items analyzed</div><div class="mono">${scope.items_analyzed ?? ""}</div>
      <div class="subtle">Time min</div><div class="mono">${scope.time_min ?? ""}</div>
      <div class="subtle">Time max</div><div class="mono">${scope.time_max ?? ""}</div>
      <div class="subtle">Input fingerprint</div><div class="mono">${fps.input_fingerprint ?? ""}</div>
      <div class="subtle">Spec fingerprint</div><div class="mono">${fps.spec_fingerprint ?? ""}</div>
      <div class="subtle">Generated UTC</div><div class="mono">${fps.generated_utc ?? ""}</div>
    `;
    details.appendChild(box);

    td.appendChild(details);
    dtr.appendChild(td);
    tbody.appendChild(dtr);
  }

  table.appendChild(tbody);
  root.appendChild(table);
}

main();
</script>
</body>
</html>
"""

def main() -> None:
    idx = load_index()

    # Determinism: do not embed dynamic values from runtime beyond the index itself.
    # We only verify it parses.
    if not isinstance(idx, dict) or "signals" not in idx:
        raise SystemExit("signals_index.json missing expected keys")

    OUT_HTML.write_text(html_template(), encoding="utf-8")
    print(f"Wrote: {OUT_HTML}")

if __name__ == "__main__":
    main()

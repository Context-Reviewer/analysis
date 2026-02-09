import json
import pathlib
import csv
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
INP = ROOT / "fb_extract_out" / "sean_context_enriched.jsonl"
OUT_JSON = ROOT / "fb_extract_out" / "sean_timeline.json"
OUT_CSV = ROOT / "fb_extract_out" / "sean_timeline.csv"

rows = []

if INP.exists():
    with INP.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            assert row.get("source") == "graphql_v2"
            ts = row.get("created_time_iso")
            assert ts
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            row["_created_dt"] = dt.isoformat()
            rows.append(row)

rows.sort(key=lambda r: r["_created_dt"])

OUT_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

# Stable headers (even if empty)
headers = ["legacy_fbid", "post_id", "author", "created_time_iso", "_created_dt", "body", "source"]

with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=headers)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k) for k in headers})

print("[timeline] complete:", len(rows), "rows")

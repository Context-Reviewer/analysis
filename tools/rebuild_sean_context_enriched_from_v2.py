import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
INP = ROOT / "fb_extract_out" / "comments_graphql_v2.jsonl"
OUT = ROOT / "fb_extract_out" / "sean_context_enriched.jsonl"

count = 0

with INP.open(encoding="utf-8") as f, OUT.open("w", encoding="utf-8") as o:
    for line in f:
        if not line.strip():
            continue
        row = json.loads(line)

        assert row.get("source") == "graphql_v2"
        assert row.get("created_time_iso")
        body = row.get("body") or {}
        assert isinstance(body, dict) and body.get("text")

        author = row.get("author")
        if author != "Sean Roy":
            continue

        o.write(json.dumps(row, ensure_ascii=False) + "\n")
        count += 1

print("[enriched] complete:", count, "rows")

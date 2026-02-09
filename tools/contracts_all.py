import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]

# ---------- LEGACY SCRAPER GUARD ----------
ILLEGAL_MARKERS = [
    "tools.legacy_scraper",
    "tools/legacy_scraper",
    "tools\\legacy_scraper",
]

this_file = pathlib.Path(__file__).resolve()

for py in ROOT.rglob("*.py"):
    if py.resolve() == this_file:
        continue
    if "legacy_scraper" in py.parts:
        continue

    text = py.read_text(encoding="utf-8", errors="ignore")
    for m in ILLEGAL_MARKERS:
        if m in text:
            raise AssertionError(f"Illegal legacy scraper reference in {py}: contains '{m}'")

# ---------- LOAD SUMMARY ----------
summary_path = ROOT / "fb_extract_out" / "comments_graphql_v2_summary.json"
if not summary_path.exists():
    raise AssertionError("Missing comments_graphql_v2_summary.json")

summary = json.loads(summary_path.read_text(encoding="utf-8"))

posts_covered = summary.get("posts_covered")
post_ids = summary.get("post_ids", [])

assert isinstance(posts_covered, int)
assert isinstance(post_ids, list)
assert len(post_ids) == posts_covered
assert len(set(post_ids)) == posts_covered

# ---------- RUN.JSON CONSISTENCY ----------
run_dir = ROOT / "fb_extract_out" / "netlog_queue_urls"
run_post_ids = set()

for run in run_dir.rglob("run.json"):
    try:
        data = json.loads(run.read_text(encoding="utf-8"))
    except Exception:
        continue
    pid = data.get("post_id")
    if pid:
        run_post_ids.add(str(pid))

for pid in post_ids:
    assert str(pid) in run_post_ids, f"Post {pid} missing run.json provenance"

print("[contracts] ALL PASSED")

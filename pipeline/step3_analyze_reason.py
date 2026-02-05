# step3_analyze_reason.py
# Step 3:
# - Load fb_extract_out/sean_timeline.json
# - Rule-based topic tagging (transparent + editable)
# - VADER sentiment analysis (metadata only, not for filtering/sorting)
# - Write:
#     fb_extract_out/sean_topics.csv
#     fb_extract_out/sean_report.md
#
# Governance:
# - Sentiment is descriptive metadata ONLY
# - ❌ No filtering by sentiment
# - ❌ No sorting by sentiment
# - ❌ No boosting/weighting by sentiment

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

# VADER for sentiment metadata
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


OUT_DIR = Path("fb_extract_out")
SRC_TIMELINE = OUT_DIR / "sean_timeline.json"

OUT_TOPICS_CSV = OUT_DIR / "sean_topics.csv"
OUT_REPORT_MD = OUT_DIR / "sean_report.md"

PREVIEW_CHARS = 260
TOP_POSTS_PER_TOPIC = 8


# -----------------------------
# Topic rules (edit freely)
# -----------------------------
# Each topic is triggered by ANY keyword/regex match.
# Keep these concrete. Avoid vague words that match everything.

TOPIC_RULES: Dict[str, List[str]] = {
    "immigration_ice": [
        r"\bice\b", r"\bdeport", r"\bborder\b", r"\billegal\b", r"\bimmigra",
        r"\basylum\b", r"\bsanctuary\b"
    ],
    "crime_drugs": [
        r"\bmeth\b", r"\bfent\b", r"\bfentanyl\b", r"\bheroin\b", r"\bpill\b",
        r"\btrafficks?[\w]*\b", r"\bdealer\b", r"\boverdose\b"
    ],
    "sex_offense_minors": [
        r"\bminor\b", r"\bunderage\b", r"\bchild\b", r"\bkid\b",
        r"\bped(o|ophile)\b", r"\brape\b", r"\bmolest\b", r"\bsex offender\b"
    ],
    "politics_culturewar": [
        r"\btrump\b", r"\bbiden\b", r"\bdemocrat\b", r"\brepublican\b",
        r"\bleft(ist|oids?)\b", r"\bright[-\s]?wing\b", r"\bconservative\b",
        r"\bliberal\b", r"\bpolitic"
    ],
    "religion_morality": [
        r"\bbible\b", r"\bjesus\b", r"\bchrist\b", r"\bchurch\b",
        r"\bgod\b", r"\bsin\b", r"\bmoral"
    ],
    "relationships_gender": [
        r"\bwoman\b", r"\bwomen\b", r"\bman\b", r"\bmen\b", r"\bwife\b",
        r"\bhusband\b", r"\bmarriage\b", r"\bdivorce\b", r"\bgirlfriend\b",
        r"\bboyfriend\b", r"\bdating\b"
    ],
    "money_debt_work": [
        r"\bdebt\b", r"\bloan\b", r"\bcredit\b", r"\bpaycheck\b", r"\bwage\b",
        r"\bsalary\b", r"\brent\b", r"\bmortgage\b", r"\btaxes?\b",
        r"\bjob\b", r"\bwork\b"
    ],
    "violence_threats": [
        r"\bshoot\b", r"\bgun\b", r"\bweapon\b", r"\bkill\b", r"\bhang\b",
        r"\bviolent\b", r"\battack\b"
    ],
    "meta_social_media": [
        r"\bfacebook\b", r"\breddit\b", r"\bonline\b", r"\bcomment\b",
        r"\bpost\b", r"\balgorithm\b", r"\bcens(or|orship)\b"
    ],
    "general_rage_mockery": [
        r"\bfuck\b", r"\bshit\b", r"\bidiot\b", r"\bstupid\b", r"\bdumb\b",
        r"\bmoron\b", r"\bretard\b", r"\bparasite\b", r"\btrash\b"
    ],
}


def compile_rules(rules: Dict[str, List[str]]) -> Dict[str, List[re.Pattern]]:
    out: Dict[str, List[re.Pattern]] = {}
    for topic, pats in rules.items():
        out[topic] = [re.compile(p, flags=re.IGNORECASE) for p in pats]
    return out


RULES = compile_rules(TOPIC_RULES)


def preview(text: str, n: int = PREVIEW_CHARS) -> str:
    t = (text or "").strip()
    t = re.sub(r"\s+", " ", t)
    if len(t) <= n:
        return t
    return t[:n].rstrip() + "…"


def load_timeline(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Timeline JSON is not a list.")
    return [x for x in data if isinstance(x, dict)]


def tag_topics(body: str) -> List[str]:
    hits: List[str] = []
    for topic, pats in RULES.items():
        for p in pats:
            if p.search(body or ""):
                hits.append(topic)
                break
    return hits


def score_post(body: str) -> int:
    """
    Simple salience score: length + profanity density.
    Used only to rank representative posts per topic.
    NOTE: Does NOT use sentiment per governance.
    """
    b = body or ""
    length_score = min(len(b), 2000)  # cap
    profanity = len(re.findall(r"\b(fuck|shit|idiot|stupid|dumb|moron|retard|parasite|trash)\b", b, flags=re.I))
    return length_score + profanity * 150


# Initialize VADER analyzer (once)
VADER = SentimentIntensityAnalyzer()


def get_sentiment(body: str) -> Dict[str, float]:
    """Compute VADER sentiment scores. Returns dict with compound, pos, neg, neu."""
    scores = VADER.polarity_scores(body or "")
    return {
        "sentiment_compound": scores.get("compound", 0.0),
        "sentiment_pos": scores.get("pos", 0.0),
        "sentiment_neg": scores.get("neg", 0.0),
        "sentiment_neu": scores.get("neu", 0.0),
    }


def main() -> None:
    if not SRC_TIMELINE.exists():
        raise SystemExit(f"Missing: {SRC_TIMELINE.resolve()}")

    posts = load_timeline(SRC_TIMELINE)
    if not posts:
        raise SystemExit("No posts loaded from timeline.")

    # Tag posts
    rows: List[Dict[str, Any]] = []
    topic_counts = defaultdict(int)
    per_topic_posts: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)

    for i, p in enumerate(posts, start=1):
        body = (p.get("body") or "").strip()
        permalink = p.get("permalink") or ""
        time_key = p.get("timestamp_parsed") or p.get("captured_at") or ""
        raw_ts = p.get("timestamp_raw") or ""

        topics = tag_topics(body)
        if not topics:
            topics = ["uncategorized"]

        sc = score_post(body)
        
        # Compute sentiment metadata (descriptive only)
        sentiment = get_sentiment(body)

        row = {
            "i": i,
            "item_type": p.get("item_type", "post"),
            "time": time_key,
            "timestamp_raw": raw_ts,
            "permalink": permalink,
            "topics": "|".join(topics),
            "body_len": len(body),
            "preview": preview(body),
            **sentiment,  # Add sentiment columns
        }
        rows.append(row)

        for t in topics:
            topic_counts[t] += 1
            per_topic_posts[t].append((sc, p))

    # Write topics CSV (with sentiment columns)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_TOPICS_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["i", "item_type", "time", "timestamp_raw", "permalink", "topics", "body_len", 
                    "sentiment_compound", "sentiment_pos", "sentiment_neg", "sentiment_neu", "preview"])
        for r in rows:
            w.writerow([
                r["i"], r["item_type"], r["time"], r["timestamp_raw"], r["permalink"], r["topics"], r["body_len"],
                round(r["sentiment_compound"], 3), round(r["sentiment_pos"], 3), 
                round(r["sentiment_neg"], 3), round(r["sentiment_neu"], 3),
                r["preview"]
            ])

    # Build report.md
    topic_sorted = sorted(topic_counts.items(), key=lambda kv: kv[1], reverse=True)

    lines: List[str] = []
    lines.append("# Sean Roy — Post Corpus Report")
    lines.append("")
    lines.append(f"- Source file: `{SRC_TIMELINE.name}`")
    lines.append(f"- Posts analyzed: **{len(posts)}**")
    lines.append(f"- Topics file: `{OUT_TOPICS_CSV.name}`")
    lines.append("")
    lines.append("## Topic frequency")
    lines.append("")
    for t, c in topic_sorted:
        lines.append(f"- **{t}**: {c}")
    lines.append("")

    lines.append("## Representative posts per topic")
    lines.append("")
    for t, c in topic_sorted:
        lines.append(f"### {t} ({c})")
        picks = sorted(per_topic_posts[t], key=lambda x: x[0], reverse=True)[:TOP_POSTS_PER_TOPIC]
        for rank, (sc, p) in enumerate(picks, start=1):
            time_key = p.get("timestamp_parsed") or p.get("captured_at") or ""
            link = p.get("permalink") or ""
            body = (p.get("body") or "").strip()
            lines.append(f"{rank}. `{time_key}` — {preview(body)}")
            lines.append(f"   - link: {link}")
        lines.append("")

    # Pattern / loop hints
    lines.append("## Pattern notes (auto-generated)")
    lines.append("")
    top_topics = [t for t, _ in topic_sorted[:5]]
    lines.append(f"- Most common topics: {', '.join(top_topics)}")
    lines.append("- If one topic dominates, that’s usually the 'core loop' you’re reacting to.")
    lines.append("- If multiple topics co-occur repeatedly (e.g., politics + crime + rage), that’s a signature pattern.")
    lines.append("")

    # Sentiment distribution (metadata only, not decision signal)
    lines.append("## Sentiment distribution (descriptive metadata)")
    lines.append("")
    neg_count = sum(1 for r in rows if r["sentiment_compound"] < -0.5)
    pos_count = sum(1 for r in rows if r["sentiment_compound"] > 0.5)
    neu_count = len(rows) - neg_count - pos_count
    lines.append(f"- Strongly negative (compound < -0.5): **{neg_count}**")
    lines.append(f"- Strongly positive (compound > 0.5): **{pos_count}**")
    lines.append(f"- Neutral/mixed: **{neu_count}**")
    lines.append("")
    lines.append("> **Note:** Sentiment scores are descriptive metadata. They are NOT used to filter, rank, or prioritize content.")
    lines.append("")

    # Sanity check section
    lines.append("## Sanity check: what this data does and doesn’t prove")
    lines.append("")
    lines.append("**This data can support:**")
    lines.append("- Identifying repeated themes, tone, and escalation patterns across posts.")
    lines.append("- Confirming whether this is 'one-off' behavior or a repeated pattern.")
    lines.append("- Giving you grounded examples to reference when explaining your decision-making.")
    lines.append("")
    lines.append("**This data does NOT prove:**")
    lines.append("- Motives, intent, or off-platform behavior.")
    lines.append("- Truth/falsity of claims made in posts.")
    lines.append("- Anything about posts you cannot see (deleted/private/blocked content).")
    lines.append("")

    # Draft reason statement scaffold
    lines.append("## Draft reason statement scaffold (edit in plain language)")
    lines.append("")
    lines.append("Use this as a starting point. Keep it short and factual.")
    lines.append("")
    lines.append("> I collected Sean Roy’s posts in the group because I needed to know whether what I was seeing was a one-off or a repeated pattern.")
    lines.append("> After reviewing the posts together, the pattern I see most consistently is: **[top theme(s) here]**.")
    lines.append("> The part that matters to me is: **[tone/targets/escalation/obsession]**.")
    lines.append("> The point of doing this wasn’t to argue or ‘win’ online. It was to clarify my own decision about **[mute/avoid/report/disengage/other]** based on evidence I could verify.")
    lines.append("> This dataset is limited to what was visible to me in the group at the time I collected it, but it’s enough for me to make a clear call.")
    lines.append("")

    OUT_REPORT_MD.write_text("\n".join(lines), encoding="utf-8")

    print("Step 3 complete.")
    print(f"Wrote: {OUT_TOPICS_CSV.resolve()}")
    print(f"Wrote: {OUT_REPORT_MD.resolve()}")
    print("")
    print("Top topics:")
    for t, c in topic_sorted[:10]:
        print(f"  {c:>3}  {t}")


if __name__ == "__main__":
    main()

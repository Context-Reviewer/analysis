import json
import os
import re
import sys
import argparse
import hashlib
from collections import defaultdict, Counter
from datetime import datetime, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# =========================
# CONFIG
# =========================
DEFAULT_INPUT_FILE = r"C:\Users\lwpar\Desktop\fb_extract_out\sean_fusion_clean.jsonl"
DEFAULT_OUTPUT_JSON = r"C:\Users\lwpar\Desktop\analysis\docs\data\report.json"
TARGET_AUTHOR = "Sean's Post"

# =========================
# STEP 1: TOPIC TAXONOMY
# =========================
TAXONOMY = {
    "geopolitics_israel": [
        "israel", "gaza", "palestine", "hamas", "zionist", "jew", "idf", "netanyahu"
    ],
    "race_identity": [
        "white", "black", "race", "racist", "dei", "crt", "woke", "minority", "heritage", "culture"
    ],
    "religion_christianity": [
        "god", "bible", "jesus", "christ", "church", "pray", "sin", "fornication", "pastor"
    ],
    "religion_other": [
        "jew", "muslim", "islam", "buddhist", "hindu", "religion"
    ],
    "politics_domestic": [
        "biden", "trump", "democrat", "republican", "tax", "election", "vote", "congress", "pelosi", "harris"
    ],
    "military_culture": [
        "military", "veteran", "pow", "mia", "service", "flag", "troop", "deployment", "base", "army", "navy", "air force"
    ],
    "self_portrayal_values": [
        "i believe", "my opinion", "i dont condone", "i don't condone", "i never said", 
        "neutral", "fair", "tolerance", "hate", "love everyone"
    ]
}

# =========================
# HELPERS
# =========================
def get_topics(text):
    text = text.lower()
    found = []
    # Sort keys for deterministic order
    for topic in sorted(TAXONOMY.keys()):
        keywords = TAXONOMY[topic]
        if any(k in text for k in keywords):
            found.append(topic)
    return found

def get_stable_id(text):
    """Generate a consistent ID based on text content since we lack DB IDs."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:8]

# =========================
# ANALYSIS ENGINE
# =========================
def main():
    parser = argparse.ArgumentParser(description="Behavioral Analysis Engine")
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE, help="Path to input JSONL")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_JSON, help="Path to output JSON")
    parser.add_argument("--min-items", type=int, default=50, help="Minimum items required to proceed")
    parser.add_argument("--allow-small", action="store_true", help="Bypass min-items check")
    
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"File not found: {args.input}")
        return

    analyzer = SentimentIntensityAnalyzer()
    
    # Data Loading
    records = []
    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip(): records.append(json.loads(line))
            
    # Safety Check
    if len(records) < args.min_items and not args.allow_small:
        print(f"Input too small (<{args.min_items}). Extraction likely incomplete.")
        print(f"Input: {args.input}")
        print(f"Count: {len(records)}")
        sys.exit(2)
            
    # --- PROCESSING ---
    processed_topics = defaultdict(list) # topic -> list of dicts {id, sent, text}
    intrusion_matches = []
    claims = []
    dates = []
    
    total_comments_checked_for_intrusion = 0
    sensitive_topics = ["race_identity", "religion_christianity", "geopolitics_israel"]

    # Pre-process for sorting check
    # We want stable processing, input is generally stable if file doesn't change, 
    # but let's just process linearly.
    
    for r in records:
        c_text = r["comment"]["text"]
        p_text = r["post"]["text"]
        timestamp = r.get("meta", {}).get("captured_at")
        
        if timestamp:
            dates.append(timestamp)

        item_id = get_stable_id(c_text)
        
        # 1. Sentiment
        vs = analyzer.polarity_scores(c_text)
        comp = vs['compound']
        
        # 2. Topic Classification
        c_topics = get_topics(c_text)
        p_topics = get_topics(p_text)
        
        # 3. Aggregate Topic Data
        for t in c_topics:
            processed_topics[t].append({
                "id": item_id,
                "sentiment": comp,
                "text": c_text
            })
            
        # 4. Intrusion Logic
        is_intrusion = False
        if any(t in c_topics for t in sensitive_topics):
            total_comments_checked_for_intrusion += 1
            
            for t in sensitive_topics:
                if t in c_topics and t not in p_topics:
                    is_intrusion = True
                    intrusion_matches.append({
                        "id": item_id,
                        "injected_topic": t,
                        "parent_topic": sorted(p_topics)[0] if p_topics else ""
                    })
                    break 

        # 5. Self-Portrayal
        if "self_portrayal_values" in c_topics:
            claims.append({
                "id": item_id,
                "timestamp": timestamp,
                "claim_type": "explicit_value",
                "text_excerpt": c_text[:200]
            })

    # --- AGGREGATION ---
    
    # Dates
    dates.sort()
    date_min = dates[0] if dates else None
    date_max = dates[-1] if dates else None
    
    # Topics List
    final_topics = []
    for topic in sorted(processed_topics.keys()): # deterministic start
        items = processed_topics[topic]
        count = len(items)
        if count == 0: continue
        
        avg_sent = sum(x["sentiment"] for x in items) / count
        neg_items = [x for x in items if x["sentiment"] < -0.6]
        pct_neg = len(neg_items) / count
        
        final_topics.append({
            "topic": topic,
            "count": count,
            "percent_of_total": round(count / len(records), 4) if records else 0,
            "avg_sentiment": round(avg_sent, 4),
            "pct_below_neg_threshold": round(pct_neg, 4),
            "hostility_rate": round(pct_neg, 4), # Synonym for now
            "example_ids": sorted([x["id"] for x in items]) # Stable sort
        })
        
    # Sort topics by count (desc) then name (asc)
    final_topics.sort(key=lambda x: (-x["count"], x["topic"]))
    
    # Intrusion examples
    # Sort by ID for stability
    intrusion_matches.sort(key=lambda x: x["id"])
    intrusion_rate = round(len(intrusion_matches) / total_comments_checked_for_intrusion, 4) if total_comments_checked_for_intrusion else 0.0
    
    # Claims
    claims.sort(key=lambda x: x.get("timestamp") or "")
    
    # Contradictions (Placeholder logic)
    contradictions = [] 
    
    # JSON Structure
    output_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "total_items": len(records),
            "date_min": date_min,
            "date_max": date_max
        },
        "topics": final_topics,
        "intrusion": {
            "sensitive_intrusion_rate": intrusion_rate,
            "examples": intrusion_matches
        },
        "self_portrayal": {
            "claims": claims,
            "contradictions": contradictions
        }
    }
    
    # Ensure dir exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, sort_keys=False) # sort_keys=False preserves our specific ordering if dict is ordered (py3.7+)
        
    print(f"input={args.input} output={args.output} loaded={len(records)}")

if __name__ == "__main__":
    main()

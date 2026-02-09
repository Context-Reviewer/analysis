import json
import re
import os
from collections import Counter
from difflib import SequenceMatcher

INPUT_FILE = r"C:\Users\lwpar\Desktop\fb_extract_out\sean_fusion_clean.jsonl"
OUTPUT_HTML = r"C:\Users\lwpar\Desktop\fb_extract_out\sean_analysis_summary.html"

# =========================
# CONFIG / DICTIONARIES
# =========================
TARGET_AUTHOR = "Sean's Post" 

INSULT_KEYWORDS = [
    "stupid", "idiot", "dumb", "moron", "fool", "ignorant", "clown", 
    "pathetic", "liar", "hack", "shame", "disgrace", "trash", "crap",
    "bitch", "bastard", "fuck", "shit", "damn", "ass", "hell"
]

TOPIC_KEYWORDS = {
    "Religion": ["god", "bible", "jesus", "christ", "church", "pray"],
    "Politics": ["biden", "trump", "democrat", "incompetence", "pelosi", "harris", "election"],
    "Military": ["veteran", "pow", "mia", "service", "flag", "troop"],
    "General": []
}

# =========================
# METRIC FUNCTIONS
# =========================
def calculate_tone(text):
    if not text: return {}
    
    # 1. Agitation (Caps Lock)
    caps_count = sum(1 for c in text if c.isupper())
    alpha_count = sum(1 for c in text if c.isalpha())
    caps_ratio = (caps_count / alpha_count) if alpha_count > 0 else 0
    
    # 2. Aggressive Punctuation
    exclam_count = text.count("!")
    question_count = text.count("?")
    
    # 3. Profanity / Coarse Language
    lower_text = text.lower()
    insult_hits = [w for w in INSULT_KEYWORDS if w in lower_text]
    
    return {
        "word_count": len(text.split()),
        "caps_ratio": round(caps_ratio, 2),
        "exclam_count": exclam_count,
        "question_count": question_count,
        "coarse_count": len(insult_hits),
        "coarse_words": insult_hits
    }

def classify_topic(text):
    lower = text.lower()
    for topic, keywords in TOPIC_KEYWORDS.items():
        for k in keywords:
            if k in lower:
                return topic
    return "Unclassified"

# =========================
# MAIN
# =========================
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return

    records = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    print(f"Analyzing {len(records)} fused records...")

    # --- PROCESSING & GROUPING ---
    # Group by Post Text (Context)
    # Map: post_hash -> { post_text, post_author, comments: [], is_sean_thread: bool }
    threads = {}
    
    for r in records:
        c_text = r.get("comment", {}).get("text", "")
        p_text = r.get("post", {}).get("text", "") 
        p_auth = r.get("post", {}).get("author", "")
        # Create a stable key for the post
        p_key = hash(p_text)
        
        if p_key not in threads:
            threads[p_key] = {
                "post_text": p_text,
                "post_author": p_auth,
                "comments": [],
                "is_sean_thread": False
            }
        
        # Calculate heuristics
        metrics = calculate_tone(c_text)
        ratio = SequenceMatcher(None, p_text, c_text).ratio()
        
        # Check if THIS specific comment is the caption/self-post
        if ratio > 0.8:
            threads[p_key]["is_sean_thread"] = True
        
        threads[p_key]["comments"].append({
            "text": c_text,
            "metrics": metrics,
            "sim_ratio": ratio
        })

    # --- AGGREGATION & SORTING ---
    sean_threads = []
    other_threads = []
    unknown_context_threads = []
    
    for p_key, data in threads.items():
        if not data["post_text"] or len(data["post_text"]) < 5:
            # Context missing or too short -> Treat as separate issue
            # If the comment is long, it might be a self-post with failed context
            unknown_context_threads.append(data)
            continue

        if data["is_sean_thread"]:
            sean_threads.append(data)
        else:
            other_threads.append(data)
            
    # Sort Sean's threads by comment count (engagement)
    sean_threads.sort(key=lambda x: len(x["comments"]), reverse=True)
    
    # Sort Other threads by Subject's intensity
    def get_max_intensity(thread):
        return max([c["metrics"]["coarse_count"] for c in thread["comments"]] + [0])
    
    other_threads.sort(key=get_max_intensity, reverse=True)

    # --- HTML REPORT GENERATION ---
    html = f"""
    <html>
    <head>
        <title>Sean Roy - Behavioral Analysis</title>
        <style>
            body {{ font-family: "Segoe UI", sans-serif; background: #111; color: #eee; padding: 40px; max_width: 900px; margin: 0 auto; }}
            .card {{ background: #222; border: 1px solid #444; padding: 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.3); }}
            .metric {{ color: #4f4; font-weight: bold; }}
            .alert {{ color: #f55; font-weight: bold; }}
            .label {{ color: #aaa; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 5px; display: block; }}
            .post-box {{ background: #333; padding: 15px; border-left: 4px solid #777; margin-bottom: 20px; font-style: italic; white-space: pre-wrap; }}
            .comment-list {{ margin-left: 20px; border-left: 2px solid #555; padding-left: 15px; }}
            .comment-item {{ background: #2a2a2a; padding: 10px; margin-bottom: 10px; border-radius: 4px; }}
            .own-post {{ border-left-color: #4f4; background: #1a331a; }}
            .unknown-ctx {{ border-left-color: #aa4; background: #2a2a22; }}
            h1, h2 {{ border-bottom: 1px solid #555; padding-bottom: 10px; color: #ddd; }}
            .meta-tag {{ display: inline-block; background: #444; padding: 2px 6px; border-radius: 4px; font-size: 0.75em; margin-right: 5px; }}
            .methods {{ font-size: 0.85em; color: #999; border-top: 1px solid #444; margin-top: 50px; padding-top: 20px; line-height: 1.6; }}
        </style>
    </head>
    <body>
        <h1>Behavioral Analysis: {TARGET_AUTHOR}</h1>
        
        <h2>Subject-Authored Threads (Obsession Analysis)</h2>
        <p style="color:#aaa">Patterns in threads where the subject is the OP (Original Poster). Ordered by activity volume.</p>
    """
    
    if not sean_threads:
        html += "<p>No self-authored threads detected.</p>"
        
    for t in sean_threads:
        p_text = t["post_text"]
        comments = [c for c in t["comments"] if c["sim_ratio"] <= 0.8] # Filter out the caption itself
        
        html += f"""
        <div class="card own-post">
            <span class="label" style="color:#8f8">Original Post by Subject</span>
            <div class="post-box">{p_text}</div>
            
            <span class="label">Subject's Subsequent Comments ({len(comments)})</span>
            <div class="comment-list">
        """
        
        if not comments:
             html += """<div class="comment-item" style="color:#777">No follow-up comments by subject.</div>"""
        
        for c in comments:
            m = c["metrics"]
            html += f"""
            <div class="comment-item">
                {c['text']}
                <div style="font-size: 0.75em; margin-top:5px; color: #888;">
                     Caps: {int(m['caps_ratio']*100)}% | Coarse: {m['coarse_words']}
                </div>
            </div>
            """
        html += "</div></div>"

    html += f"""
        <h2>Reactive Behavior (Replies to Others)</h2>
        <p style="color:#aaa">Top interactions sorted by intensity (Coarse Language / Aggression).</p>
    """
    
    for t in other_threads[:50]: # Top 50 threads
        p_text = t["post_text"]
        p_auth = t["post_author"]
        
        html += f"""
        <div class="card">
            <span class="label">Context (Post by {p_auth})</span>
            <div class="post-box">{p_text[:400]}...</div>
            
            <span class="label">Subject's Responses ({len(t['comments'])})</span>
            <div class="comment-list">
        """
        
        for c in t["comments"]:
             m = c["metrics"]
             is_intense = m["coarse_count"] > 0 or m["caps_ratio"] > 0.3
             style = "border-left: 3px solid #f55;" if is_intense else ""
             
             html += f"""
            <div class="comment-item" style="{style}">
                {c['text']}
                <div style="font-size: 0.75em; margin-top:5px; color: #888;">
                     Caps: {int(m['caps_ratio']*100)}% | Coarse: {m['coarse_words']}
                </div>
            </div>
            """
        html += "</div></div>"

    if unknown_context_threads:
        html += f"""
            <h2>Unidentified Context</h2>
            <p style="color:#aaa">Posts where context extraction failed. Often these are Self-Posts where the caption was extracted as a comment, but the post body was missed.</p>
        """
        for t in unknown_context_threads:
             # Just show the comments as potential posts
             for c in t["comments"]:
                 html += f"""
                <div class="card unknown-ctx">
                    <span class="label" style="color:#cc4">Potential Self-Post / Missing Context</span>
                    <div style="font-size:1.1em; margin-bottom:5px;">{c['text']}</div>
                    <div style="font-size: 0.75em; color: #888;">(Context extraction failed)</div>
                </div>
                """
        
    html += """
        <div class="methods">
            <h3>Methods Note</h3>
            <p><strong>Thread Reconstruction:</strong> Comments are grouped by their parent post context (Post Text). "Subject-Authored Threads" are identified by detecting near-identical matches between the post text and the subject's commentary (indicating a caption/self-post), confirming the subject is the OP.</p>
            <p><strong>Metric Definition:</strong> This report relies on deterministic extraction of linguistic structures (capitalization ratio, punctuation density, keyword matching) to quantify tone.</p>
            <p><strong>Conservative Classification:</strong> "Coarse Language" indicates the presence of specific keywords from a controlled lexicon; it does not infer intent to harm. "High Intensity" refers strictly to typographical emphasis (Caps Lock usage > 30%).</p>
        </div>
    </body></html>
    """
    
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
        
    print(f"Report generated: {OUTPUT_HTML}")

if __name__ == "__main__":
    main()

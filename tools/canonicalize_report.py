import json
import os
import sys

TARGET_JSON = r"C:\Users\lwpar\Desktop\analysis\docs\data\report.json"

def canonicalize():
    if not os.path.exists(TARGET_JSON):
        print(f"Error: {TARGET_JSON} not found.")
        sys.exit(1)

    with open(TARGET_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Transform intrusion examples
    if "intrusion" in data and "examples" in data["intrusion"]:
        raw_examples = data["intrusion"]["examples"]
        clean_examples = []
        
        for ex in raw_examples:
            # 1. Parent Topic (First or Empty)
            p_topics = ex.get("parent_topics", [])
            # Handle case where p_topics might be the new "parent_topic" string if run repeatedly or on already clean data
            if isinstance(p_topics, str):
                 final_parent = p_topics # Already converted or existing field
            elif isinstance(p_topics, list):
                 final_parent = p_topics[0] if len(p_topics) > 0 else ""
            else:
                 final_parent = ""

            # 2. Construct clean object
            clean_obj = {
                "id": ex.get("id", ""),
                "injected_topic": ex.get("injected_topic", ""),
                "parent_topic": final_parent
            }
            clean_examples.append(clean_obj)
        
        # 3. Sort Deterministically (id -> injected -> parent)
        clean_examples.sort(key=lambda x: (x["id"], x["injected_topic"], x["parent_topic"]))
        
        # Update data
        data["intrusion"]["examples"] = clean_examples

    # Write back
    with open(TARGET_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        
    print(f"Canonicalized {TARGET_JSON}")

if __name__ == "__main__":
    canonicalize()

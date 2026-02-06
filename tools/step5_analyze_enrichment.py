#!/usr/bin/env python3
"""
Step 5: Analysis Enrichment

Implements STEP5_SPEC.md exactly. No deviations.

This step:
1. Reads Step 4 output (docs/data/report.json)
2. Reads normalized items and topic assignments from intermediate_dir
3. Computes self_portrayal and cross_topic_intrusion
4. Writes enriched report to docs/data/report.step5.json

Step 4 output remains UNTOUCHED and byte-identical.
"""
import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional


# Fixed category set (v1 lock)
SELF_PORTRAYAL_CATEGORIES = [
    "moral_identity",
    "victimhood", 
    "authority_expertise",
    "care_empathy",
    "ingroup_outgroup_positioning",
    "personal_responsibility",
    "other"
]

# Patterns for detecting self-portrayal claims by category
# Each pattern must be FIRST-PERSON and EXPLICIT (no "we X" group claims)
CATEGORY_PATTERNS: Dict[str, List[re.Pattern]] = {
    "moral_identity": [
        re.compile(r"\bi('m| am| try to be| have always been) (a )?(good|honest|fair|ethical|decent|moral)", re.I),
        re.compile(r"\bi (believe in|stand for) (doing )?(what'?s right|honesty|integrity)", re.I),
        re.compile(r"\bi('ve| have) always (tried|been|done)", re.I),
    ],
    "victimhood": [
        re.compile(r"\bi('ve| have) been (wronged|targeted|attacked|persecuted|discriminated)", re.I),
        re.compile(r"\b(they|people|everyone) (hate|attack|target|discriminate against) me\b", re.I),
        re.compile(r"\bi('m| am) (always |constantly )?(being |getting )?(attacked|targeted|bullied)", re.I),
        re.compile(r"\bno one (listens to|cares about|believes) me\b", re.I),
    ],
    "authority_expertise": [
        re.compile(r"\bi('ve| have) been doing this (for )?(\d+|many|several) (years|decades)", re.I),
        re.compile(r"\bi know what i('m| am) talking about", re.I),
        re.compile(r"\bi('m| am| was) (a |an )?(doctor|lawyer|engineer|expert|professional|specialist)", re.I),
        re.compile(r"\bi (work|worked) (in|as|at|for)", re.I),
        re.compile(r"\bi have (experience|expertise|knowledge|training) in", re.I),
        re.compile(r"\btrust me[,.]? i", re.I),
    ],
    "care_empathy": [
        re.compile(r"\bi (care|worry) about", re.I),
        re.compile(r"\bi (understand|feel for|sympathize with)", re.I),
        re.compile(r"\bi('m| am) (just )?trying to help", re.I),
        re.compile(r"\bi (love|support) (my |the )?(family|kids|children|community)", re.I),
    ],
    "ingroup_outgroup_positioning": [
        # FIRST-PERSON ONLY - "I'm a..." or "As a..." - NO group claims like "we Christians"
        re.compile(r"\bi('m| am) (a |an )?(christian|muslim|jew|atheist|conservative|liberal|veteran|immigrant)", re.I),
        re.compile(r"\bas (a |an )?(christian|muslim|jew|atheist|conservative|liberal|veteran|parent|mother|father),? i\b", re.I),
        re.compile(r"\bi('m| am) (not )?(one of|like) (them|those people)", re.I),
    ],
    "personal_responsibility": [
        re.compile(r"\bi (take|accept) (full )?responsibility", re.I),
        re.compile(r"\bi (did|made) (it|this) (myself|on my own)", re.I),
        re.compile(r"\bi (work|worked) (hard|my way)", re.I),
        re.compile(r"\bi (earned|built|created) (everything|this|my)", re.I),
        re.compile(r"\bi don'?t (make excuses|blame others)", re.I),
    ],
}


def log(msg: str) -> None:
    """Minimal logging."""
    print(f"[step5] {msg}")


def fail(msg: str, code: int = 1) -> None:
    """Log error and exit."""
    log(f"ERROR: {msg}")
    sys.exit(code)


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file."""
    if not path.exists():
        fail(f"File not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load JSONL file."""
    if not path.exists():
        fail(f"File not found: {path}")
    items = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def detect_self_portrayal_claims(
    normalized_items: List[Dict[str, Any]],
    topic_assignments: Dict[str, str]
) -> Dict[str, Any]:
    """
    Detect self-portrayal claims in normalized items.
    
    Returns self_portrayal object per schema, or structure with zero claims.
    """
    categories: Dict[str, int] = {cat: 0 for cat in SELF_PORTRAYAL_CATEGORIES}
    examples: List[Dict[str, Any]] = []
    
    for item in normalized_items:
        body = item.get("body", "") or ""
        
        # Item ID correctness: use 'permalink' (our schema) else 'id' else 'item_id' else FAIL LOUD
        item_id = item.get("permalink") or item.get("id") or item.get("item_id") or ""
        if not item_id:
            fail(f"Normalized item missing permalink/id/item_id: cannot produce traceable examples")
        
        timestamp = item.get("timestamp_parsed") or item.get("timestamp")  # May be null
        
        # Get topic for this item (topics CSV may use 'i' index or 'permalink')
        topic = topic_assignments.get(item_id, "uncategorized")
        
        # Check each category's patterns
        for category, patterns in CATEGORY_PATTERNS.items():
            for pattern in patterns:
                match = pattern.search(body)
                if match:
                    categories[category] += 1
                    
                    # Extract STRICTLY VERBATIM excerpt (exact match only, no modifications)
                    excerpt = match.group(0)
                    
                    # Spec invariant: excerpt must be non-empty
                    if not excerpt:
                        continue
                    
                    # Add example
                    examples.append({
                        "item_id": item_id,
                        "timestamp": timestamp,
                        "topic": topic,
                        "claim_category": category,
                        "excerpt": excerpt
                    })
                    
                    # Only count once per category per item
                    break
    
    # Deduplicate examples by (item_id, category) and limit to reasonable set
    seen = set()
    unique_examples = []
    for ex in examples:
        key = (ex["item_id"], ex["claim_category"])
        if key not in seen:
            seen.add(key)
            unique_examples.append(ex)
    
    # Sort examples by item_id for determinism
    unique_examples.sort(key=lambda x: (x["item_id"], x["claim_category"]))
    
    # Note: No example cap in v1 - spec does not explicitly allow sampling
    
    total_claims = sum(categories.values())
    
    return {
        "schema_version": "self_portrayal_v1",
        "total_claims": total_claims,
        "categories": categories,
        "examples": unique_examples
    }


def build_topic_assignments(topics_csv_path: Path) -> Dict[str, str]:
    """
    Build mapping of permalink -> primary topic from topics CSV.
    Topics CSV uses 'permalink' column to identify items.
    """
    import csv
    
    assignments: Dict[str, str] = {}
    
    if not topics_csv_path.exists():
        return assignments
    
    with open(topics_csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use 'permalink' as the key to match normalized items
            item_id = row.get("permalink", "")
            if not item_id:
                continue  # Skip rows without permalink
            topics = row.get("topics", "uncategorized") or "uncategorized"

            # Compliance fix: Handle both | and , delimiters (legacy vs new)
            # Deterministically pick first non-empty token
            if '|' in topics:
                parts = topics.split('|')
            else:
                parts = topics.split(',')
            
            primary = "uncategorized"
            for p in parts:
                cleaned = p.strip()
                if cleaned:
                    primary = cleaned
                    break
            
            assignments[item_id] = primary
    
    return assignments


def find_all_files_by_role(directory: Path, pattern: str) -> List[Path]:
    """
    Find ALL files matching glob pattern. Sort lexicographically for determinism.
    Returns empty list if none found.
    """
    candidates = list(directory.glob(pattern))
    # Deterministic: sort by filename lexicographically
    candidates.sort(key=lambda p: p.name)
    return candidates


def load_all_normalized_items(directory: Path) -> List[Dict[str, Any]]:
    """
    Load ALL *normalized*.jsonl files from directory, concatenating records.
    Files are processed in lexicographic order for determinism.
    """
    files = find_all_files_by_role(directory, "*normalized*.jsonl")
    if not files:
        fail("No normalized items files found in intermediate_dir")
    
    all_items: List[Dict[str, Any]] = []
    for f in files:
        log(f"  Loading: {f.name}")
        items = load_jsonl(f)
        log(f"    -> {len(items)} records")
        all_items.extend(items)
    
    return all_items


def find_file_by_role(directory: Path, pattern: str) -> Optional[Path]:
    """
    Find a file by glob pattern. If multiple exist, choose deterministically
    by lexicographic sort (NOT by mtime - that would break reproducibility).
    """
    candidates = find_all_files_by_role(directory, pattern)
    return candidates[0] if candidates else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Step 5: Analysis Enrichment per STEP5_SPEC.md"
    )
    parser.add_argument(
        "--config",
        help="Path to subject config JSON (reads paths.intermediate_dir)"
    )
    parser.add_argument(
        "--intermediate-dir",
        help="Override intermediate directory path"
    )
    parser.add_argument(
        "--report-in",
        default="docs/data/report.json",
        help="Step 4 report input path"
    )
    parser.add_argument(
        "--report-out",
        default="docs/data/report.step5.json",
        help="Step 5 enriched report output path"
    )
    args = parser.parse_args()
    
    log("Starting Step 5: Analysis Enrichment")
    
    # Determine intermediate_dir: CLI > config > default
    intermediate_dir = Path("fb_extract_out")
    if args.intermediate_dir:
        intermediate_dir = Path(args.intermediate_dir)
    elif args.config:
        config = load_json(Path(args.config))
        intermediate_dir = Path(config.get("paths", {}).get("intermediate_dir", "fb_extract_out"))
    
    report_path = Path(args.report_in)
    output_path = Path(args.report_out)
    
    log(f"Intermediate dir: {intermediate_dir}")
    log(f"Report input: {report_path}")
    log(f"Report output: {output_path}")
    
    # Find normalized items file by role (deterministic: lexicographic sort)
    normalized_path = find_file_by_role(intermediate_dir, "*normalized*.jsonl")
    if not normalized_path:
        fail("No normalized items file found in intermediate_dir")
    
    # Find topics CSV by role (deterministic: lexicographic sort)
    topics_path = find_file_by_role(intermediate_dir, "*topics*.csv")
    
    # Load Step 4 output
    log(f"Loading Step 4 output: {report_path}")
    report = load_json(report_path)
    
    # Load ALL normalized items (from all matching files, concatenated)
    log(f"Loading normalized items from: {intermediate_dir}")
    normalized_items = load_all_normalized_items(intermediate_dir)
    log(f"  Total loaded: {len(normalized_items)} items")
    
    # Build topic assignments
    topic_assignments: Dict[str, str] = {}
    if topics_path:
        log(f"Loading topic assignments: {topics_path}")
        topic_assignments = build_topic_assignments(topics_path)
    
    # Compute self-portrayal claims
    log("Computing self-portrayal claims...")
    self_portrayal = detect_self_portrayal_claims(normalized_items, topic_assignments)
    log(f"  Total claims: {self_portrayal['total_claims']}")
    log(f"  Examples: {len(self_portrayal['examples'])}")
    
    # Add to report
    # Step 5A: self_portrayal computed, cross_topic_intrusion explicitly null
    report["self_portrayal"] = self_portrayal
    report["cross_topic_intrusion"] = None  # Explicit null per spec
    
    # Write enriched report to NEW file (Step 4 output stays untouched)
    # sort_keys=True required for deterministic output
    log(f"Writing enriched report: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, sort_keys=True)
    
    log("Step 5A complete (self_portrayal)")


if __name__ == "__main__":
    main()

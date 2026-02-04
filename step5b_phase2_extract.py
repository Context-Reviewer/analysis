import time
import random
import json
import os
import re
from playwright.sync_api import sync_playwright

# ================= CONFIGURATION =================
PROFILE_DIR = r"C:\dev\fb_playwright_profile"
INPUT_FILE = "fb_extract_out/discovered_threads.txt"
OUTPUT_FILE = "fb_extract_out/sean_roy_comments.jsonl"
DEBUG_DIR = "fb_extract_out/debug"

# Ensure debug dir
os.makedirs(DEBUG_DIR, exist_ok=True)

MAX_EXPANSION_ROUNDS = 10

def human_delay(min_s=1.0, max_s=3.0):
    time.sleep(random.uniform(min_s, max_s))

def is_canonical_thread(url):
    # Matches strict pattern from Phase 1 requirements
    return re.match(r"^https://www\.facebook\.com/groups/(\d+|[\w\.]+)/(posts|permalink)/(\d+)/?$", url) is not None

def expand_thread(page):
    """
    Clicks 'View more comments', 'replies', 'See more' until exhausted or max rounds.
    """
    print("  [Expansion] Starting...")
    # Strict patterns per spec
    pattern = re.compile(r"View more comments|View replies|See more", re.IGNORECASE)
    
    for round_idx in range(MAX_EXPANSION_ROUNDS):
        # We target specific text candidates to avoid clicking random buttons
        candidates = page.locator("div[role='button'], span[role='button'], div.x1i10hfl").filter(has_text=pattern)
        
        count = candidates.count()
        if count == 0:
            break
            
        clicked_any = False
        # Try to click up to 3 visible ones per round
        for i in range(min(count, 3)):
            btn = candidates.nth(i)
            if btn.is_visible():
                try:
                    btn.scroll_into_view_if_needed()
                    btn.click(timeout=1000)
                    human_delay(0.5, 1.0)
                    clicked_any = True
                except:
                    pass
        
        if not clicked_any:
            break
        
        human_delay(1, 2)

def extract_payload(page, post_url):
    """
    Finds comments by 'Sean Roy' using Strict Ancestor+Locator Logic.
    """
    extracted_count = 0
    
    # 1. Candidate Header Match: Exact "Sean Roy"
    # We assume 'Sean Roy' is a link (author) or strong/span text.
    # filter(has_text=...) is contains. We want strictness.
    # We will use has_text but verify exact match in loop or use regex ^Sean Roy$ if possible?
    # Playwright has_text matches substring. 
    # Better: Use text=Sean Roy which matches exact text or contains.
    
    candidates = page.locator("a, span, strong").filter(has_text="Sean Roy").all()
    
    # Filter for exact match text to avoid "Sean Royerson"
    valid_candidates = []
    for c in candidates:
        if c.inner_text().strip() == "Sean Roy":
            valid_candidates.append(c)
            
    print(f"  [Extraction] Found {len(valid_candidates)} exact 'Sean Roy' candidates.")
    
    for i, candidate in enumerate(valid_candidates):
        try:
            # 2. Container Identity (Ascend + Locator Check)
            current = candidate
            container_found = None
            
            # Ascend up to 8 levels
            for _ in range(8):
                parent = current.locator("..")
                
                # LOCATOR CHECK for Footer: "Like" AND "Reply"
                # We do NOT use inner_text() scanning.
                # We check if this parent contains elements with text "Like" and "Reply"
                has_like = parent.locator("text=Like").count() > 0
                has_reply = parent.locator("text=Reply").count() > 0
                
                if has_like and has_reply:
                    container_found = parent
                    break
                current = parent
            
            if container_found:
                # 3. Payload Extraction (Inside Container, Exclusion)
                # We need the text of the body.
                # Since we can't use BeautifulSoup, and text is mixed.
                # Strategy: Get full text, then remove known artifacts.
                
                full_text = container_found.inner_text()
                
                # Exclusion List
                exclusions = ["Sean Roy", "Like", "Reply", "See more", "Edit", "Delete", "Hide"]
                
                # Simple Line filtering or String replacement?
                # "Sean Roy" is the header. "Like Reply 2h" is the footer.
                # The Body is what remains.
                
                clean_text = full_text
                for exc in exclusions:
                    clean_text = clean_text.replace(exc, "")
                
                # Remove timestamps (heuristic: "1h", "2d", "Yesterday")
                clean_text = re.sub(r"\b\d+[hmwdys]\b", "", clean_text)
                clean_text = clean_text.strip()
                
                if not clean_text:
                    # Payload empty after cleaning
                    # Log failure reason
                    log_failure(page, post_url, "PAYLOAD_EMPTY_AFTER_CLEAN")
                    continue

                payload = {
                    "post_url": post_url,
                    "author": "Sean Roy",
                    "comment_text": clean_text,
                    "timestamp": None,
                    "debug_evidence": {
                        "locator_hint": f"candidate_index_{i}",
                        "snippet_hash": str(hash(clean_text))
                    }
                }
                
                # Append to JSONL
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                
                extracted_count += 1
            else:
                # Log Failure: Container Not Found
                log_failure(page, post_url, "CONTAINER_NOT_FOUND")

        except Exception as e:
            # Log Unexpected
            pass

    # Global Failure for this Thread
    if len(valid_candidates) > 0 and extracted_count == 0:
         log_failure(page, post_url, "NO_EXTRACTED_DESPITE_CANDIDATES")

    return extracted_count

def log_failure(page, url, reason):
    """
    Dumps deterministic debug artifacts.
    """
    print(f"    [Failure Log] {reason}")
    thread_id = url.split("/")[-2] if "/posts/" in url else "unknown"
    
    # File names
    html_path = f"{DEBUG_DIR}/phase2_{thread_id}_{reason}.html"
    png_path = f"{DEBUG_DIR}/phase2_{thread_id}_{reason}.png"
    
    # Avoid overwriting if multiple errors in same thread? 
    # Spec says "Deterministic names". We stick to this.
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        page.screenshot(path=png_path)
    except:
        pass

def main():
    print("=== Phase 2: Extraction (Strict Architecture) ===")
    
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(urls)} thread URLs.")
    
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            viewport={"width": 1280, "height": 900},
             args=["--disable-notifications"]
        )
        page = context.pages[0] if context.pages else context.new_page()

        total_extracted = 0
        
        for idx, url in enumerate(urls):
            print(f"\nProcessing [{idx+1}/{len(urls)}]: {url}")
            
            # GUARD
            if not is_canonical_thread(url):
                print("  [Guard] Skipped: Non-canonical URL")
                continue
                
            try:
                page.goto(url, wait_until="domcontentloaded")
                human_delay(2, 4)
                
                # Expansion
                expand_thread(page)
                
                # Extraction
                count = extract_payload(page, url)
                total_extracted += count
                print(f"  -> Extracted {count} items.")
                
            except Exception as e:
                print(f"  -> Error: {e}")

        print("\n" + "="*30)
        print("PHASE 2 SUMMARY")
        print(f"Threads Processed: {len(urls)}")
        print(f"Total Comments:    {total_extracted}")
        print(f"Output File:       {OUTPUT_FILE}")
        print("="*30)
        
        context.close()

if __name__ == "__main__":
    main()
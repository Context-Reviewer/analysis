import time
import random
import os
import re
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright

# ================= CONFIGURATION =================
PROFILE_DIR = r"C:\dev\fb_playwright_profile"
OUTPUT_FILE = "fb_extract_out/discovered_threads.txt"
MAX_SCROLL_ROUNDS = 20

# Strict Canonical Regex: https://www.facebook.com/groups/<gid>/(posts|permalink)/<id>/?
CANONICAL_PATTERN = re.compile(
    r"^https://www\.facebook\.com/groups/(\d+|[\w\.]+)/(posts|permalink)/(\d+)/?$"
)

# Counters
COUNTERS = {
    "seen_cards": 0,
    "thread_urls_ok": 0,
    "rejected_nonthread": 0,
    "media_viewer_seen": 0,
    "media_resolved": 0,
    "media_unresolved": 0
}

def human_delay(min_s=1.0, max_s=3.0):
    time.sleep(random.uniform(min_s, max_s))

def parse_group_search_url(url):
    """
    Validates that URL matches /groups/<gid>/search?q=...
    Returns (gid, q) or raises ValueError.
    """
    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")
    
    # Expected: ['groups', '<gid>', 'search']
    if len(path_parts) < 3 or path_parts[0] != "groups" or path_parts[-1] != "search":
        raise ValueError("URL path must match /groups/<gid>/search")
        
    qs = parse_qs(parsed.query)
    if "q" not in qs or not qs["q"]:
        # Strict: q param must exist (even if empty? Spec says 'include q=')
        # We enforce it must exist.
        raise ValueError("URL must contain 'q' query parameter")
        
    gid = path_parts[1]
    q = qs["q"][0]
    return gid, q

def assert_is_group_search_surface(page, gid):
    """
    Verifies we are on the search surface.
    """
    # Check for search input or "Results for..." header
    # Or strict check that we are still in group context
    # This is a heuristic assertion
    try:
        # Check URL
        curr = page.url
        if f"/groups/{gid}/search" not in curr:
             print("  [Guard] URL Drift detected.")
             return False
             
        # Check DOM element
        # "Search this group" input or "All" tab selection
        if page.locator("div[role='main']").count() == 0:
            print("  [Guard] Main role not found.")
            return False
            
        return True
    except:
        return False

def canonicalize_thread_url(url, gid):
    """
    Returns canonical URL or None.
    Force https://www.facebook.com
    Force trailing slash.
    """
    if not url: return None
    
    # Handle relative
    if url.startswith("/"):
        url = "https://www.facebook.com" + url
        
    # Strip query/fragment
    base = url.split("?")[0].split("#")[0]
    
    # Ensure trailing slash
    if not base.endswith("/"):
        base += "/"
        
    # Match Regex
    match = CANONICAL_PATTERN.match(base)
    if not match:
        return None
        
    # Optional: Verify GID matches?
    # match.group(1) is gid from url. It might be different (vanity vs id).
    # We accept it if it matches the pattern.
    return base

def resolve_media_inline(page, media_url):
    """
    Opens media viewer, resolves to thread URL, returns canonical or None.
    """
    print(f"    [Media Resolution] Opening {media_url[:50]}...")
    try:
        # We need to open in a way that doesn't lose our scroll position ideally,
        # but Playwright page object is single tab.
        # Strict inline processing: We goto, resolve, and would have to go back?
        # OR: Open new page (tab)
        
        with page.context.new_page() as media_page:
            media_page.goto(media_url, wait_until="domcontentloaded")
            human_delay(2, 3)
            
            # Look for "View post", "See post", "View on Facebook"
            candidates = media_page.locator("a").filter(has_text=re.compile(r"View post|See post|View on Facebook", re.IGNORECASE))
            
            count = candidates.count()
            if count > 0:
                href = candidates.nth(0).get_attribute("href")
                return href
                
            # Fallback: Timestamp/Permalink
            perm = media_page.locator("a[href*='/posts/'], a[href*='/permalink/']")
            if perm.count() > 0:
                return perm.first.get_attribute("href")
                
    except Exception as e:
        print(f"    [Media Resolution] Error: {e}")
        
    return None

def process_search_results(page, gid, discovered_urls):
    """
    Scans visible search result cards.
    """
    # Locator for search result cards. 
    # Valid cards usually have role="article" or specific class wrappers in feed.
    # We iterate all role=article to be safe, then validate.
    cards = page.locator("div[role='article']").all()
    
    new_found = 0
    
    for card in cards:
        # Check if we already processed this card? 
        # Hard to do without unique ID on card. We rely on URL dedupe.
        COUNTERS["seen_cards"] += 1
        
        # 1. Look for Thread Link
        # Usually timestamp or title
        # Strategy: Find any link matching /posts/ or /permalink/ inside the card
        
        thread_link_loc = card.locator("a[href*='/posts/'], a[href*='/permalink/']")
        if thread_link_loc.count() > 0:
            raw_url = thread_link_loc.first.get_attribute("href")
            canon = canonicalize_thread_url(raw_url, gid)
            if canon:
                if canon not in discovered_urls:
                    discovered_urls.add(canon)
                    COUNTERS["thread_urls_ok"] += 1
                    new_found += 1
                    print(f"    [Found] {canon}")
                continue # Done with this card
        
        # 2. Look for Media Link if no thread link
        media_link_loc = card.locator("a[href*='photo.php'], a[href*='/photos/'], a[href*='/videos/'], a[href*='story.php'], a[href*='/watch/']")
        if media_link_loc.count() > 0:
            raw_media = media_link_loc.first.get_attribute("href")
            # Resolve Inline
            COUNTERS["media_viewer_seen"] += 1
            if raw_media.startswith("/"): raw_media = "https://www.facebook.com" + raw_media
            
            resolved_url = resolve_media_inline(page, raw_media)
            canon = canonicalize_thread_url(resolved_url, gid)
            
            if canon:
                COUNTERS["media_resolved"] += 1
                if canon not in discovered_urls:
                    discovered_urls.add(canon)
                    COUNTERS["thread_urls_ok"] += 1
                    new_found += 1
                    print(f"    [Resolved] {canon}")
            else:
                COUNTERS["media_unresolved"] += 1
                # Bucket as unresolved (internal counter only)
            continue
            
        COUNTERS["rejected_nonthread"] += 1

    return new_found

def main():
    print("=== Phase 1: Discovery (Strict Architecture) ===")
    
    # INPUT
    search_url = input("Enter Facebook Group SEARCH URL: ").strip()
    
    # GUARD
    try:
        gid, q = parse_group_search_url(search_url)
        print(f"  [Guard] Valid URL. GID: {gid}, Query: {q}")
    except ValueError as e:
        print(f"  [Guard] FAIL: {e}")
        print("  EXITING with zero output.")
        # Ensure output is empty
        with open(OUTPUT_FILE, "w") as f: pass
        return

    print(f"Initializing Playwright...")
    
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            viewport={"width": 1280, "height": 900},
            args=["--disable-notifications"]
        )
        page = context.pages[0] if context.pages else context.new_page()
        
        print(f"Navigating to: {search_url}")
        page.goto(search_url, wait_until="domcontentloaded")
        
        # DOM GUARD
        if not assert_is_group_search_surface(page, gid):
            print("  [Guard] FAIL: Not a group search surface.")
            context.close()
            with open(OUTPUT_FILE, "w") as f: pass
            return

        input("Verify page loaded (Manual Auth if needed). Press Enter to start...")

        discovered_urls = set()
        
        # SCROLL LOOP
        print("Starting Discovery Loop...")
        for i in range(MAX_SCROLL_ROUNDS):
            print(f"  Round {i+1}/{MAX_SCROLL_ROUNDS}")
            
            found = process_search_results(page, gid, discovered_urls)
            
            # Scroll
            page.keyboard.press("End")
            human_delay(2, 4)
            
            # Optional: Check for "End of results" text?
        
        # OUTPUT
        print("\nWriting output...")
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        sorted_urls = sorted(list(discovered_urls))
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for url in sorted_urls:
                f.write(url + "\n")
                
        print("\n" + "="*30)
        print("PHASE 1 SUMMARY")
        print(f"Cards Processed:  {COUNTERS['seen_cards']}")
        print(f"Thread URLs OK:   {COUNTERS['thread_urls_ok']}")
        print(f"Rejected (Non):   {COUNTERS['rejected_nonthread']}")
        print(f"Media Seen:       {COUNTERS['media_viewer_seen']}")
        print(f"Media Resolved:   {COUNTERS['media_resolved']}")
        print(f"Media Unresolved: {COUNTERS['media_unresolved']}")
        print(f"Total Output:     {len(sorted_urls)}")
        print(f"File:             {OUTPUT_FILE}")
        print("="*30)
        
        context.close()

if __name__ == "__main__":
    main()
"""
Phase 1: Discovery - Facebook Group Search -> Post Thread URLs
Usage: python step5b_phase1_discover.py

Invariants:
- Output ONLY /posts/ or /permalink/ URLs
- Media viewers are resolved or rejected
- Deterministic counters logged at end
"""
import asyncio
import json
import os
import re
from playwright.async_api import async_playwright

# =========================
# CONFIG
# =========================
SEARCH_URL = "https://www.facebook.com/groups/3970539883001618/search?q=sean%20roy"
OUTPUT_DIR = r"C:\Users\lwpar\Desktop\fb_extract_out"
DISCOVERED_TXT = os.path.join(OUTPUT_DIR, "discovered_threads.txt")
PROFILE_DIR = r"C:\dev\fb_playwright_profile"
GROUP_ID = "3970539883001618"

MAX_SCROLLS = 30
SCROLL_DELAY = 2.0

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# URL CLASSIFICATION
# =========================
def is_group_search_url(url: str) -> bool:
    return f"/groups/{GROUP_ID}/search" in url

def is_valid_thread_url(url: str) -> bool:
    """Only /posts/ or /permalink/ in our group"""
    return (f"/groups/{GROUP_ID}/posts/" in url or 
            f"/groups/{GROUP_ID}/permalink/" in url)

def is_media_viewer(url: str) -> bool:
    patterns = ["/photo/", "/photos/", "/video/", "/videos/", 
                "story.php", "permalink.php", "/watch/", "/reel/"]
    return any(p in url for p in patterns)

def canonicalize(url: str) -> str:
    """Strip query params, ensure trailing slash"""
    base = url.split('?')[0].split('&')[0]
    if not base.endswith('/'):
        base += '/'
    return base

# =========================
# MEDIA RESOLVER
# =========================
async def resolve_media_to_thread(context, media_url: str) -> str | None:
    """Open media viewer, find underlying post thread URL"""
    try:
        page = await context.new_page()
        await page.goto(media_url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        
        # Strategy 1: Find "View post" or "See post" link
        view_post_links = page.locator("a:has-text('View post'), a:has-text('See post'), a:has-text('View on Facebook')")
        count = await view_post_links.count()
        
        if count > 0:
            href = await view_post_links.first.get_attribute("href")
            if href and is_valid_thread_url(href):
                await page.close()
                return canonicalize(href)
        
        # Strategy 2: Find any /posts/ or /permalink/ anchor
        thread_links = page.locator(f"a[href*='/groups/{GROUP_ID}/posts/'], a[href*='/groups/{GROUP_ID}/permalink/']")
        count = await thread_links.count()
        
        if count > 0:
            href = await thread_links.first.get_attribute("href")
            await page.close()
            return canonicalize(href) if href else None
        
        await page.close()
        return None
        
    except Exception as e:
        print(f"   [Resolve Error] {e}")
        return None

# =========================
# MAIN DISCOVERY LOOP
# =========================
async def run_discovery():
    print("=" * 60)
    print("PHASE 1: DISCOVERY")
    print("=" * 60)
    print(f"Target: {SEARCH_URL}")
    print(f"Profile: {PROFILE_DIR}")
    
    # Counters
    stats = {
        "seen_links": 0,
        "thread_urls_ok": 0,
        "rejected_nonthread": 0,
        "media_viewer_seen": 0,
        "media_resolved": 0,
        "media_unresolved": 0
    }
    
    discovered = set()
    seen_raw = set()
    media_queue = []
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = await context.new_page()
        
        # Navigate to search
        print("[Nav] Going to search page...")
        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)
        
        # GUARD: Verify search surface
        if not is_group_search_url(page.url):
            print(f"[GUARD FAIL] Not on search surface: {page.url}")
            await page.screenshot(path=os.path.join(OUTPUT_DIR, "guard_fail_discovery.png"))
            await context.close()
            return
        
        print("[GUARD OK] On search surface")
        
        # Alternative strategy: Click into posts and capture URLs
        # Facebook search results don't have /posts/ links directly - we must click
        print("[Strategy] Clicking into search result posts...")
        
        # First, collect post cards by finding clickable areas
        # Look for timestamp links or "See more" within posts
        for scroll_num in range(MAX_SCROLLS):
            print(f"\n--- Scroll {scroll_num + 1}/{MAX_SCROLLS} ---")
            
            # Look for user links with the group context (these lead to post view)
            user_links = page.locator(f"a[href*='/groups/{GROUP_ID}/user/']")
            count = await user_links.count()
            print(f"   Found {count} user links in group context")
            
            # Also look for any clickable timestamps or post areas
            # These often have aria-label with timestamp or "commented"
            post_areas = page.locator("div[role='article'] a[href*='/groups/']")
            post_count = await post_areas.count()
            print(f"   Found {post_count} post area links")
            
            # Collect unique post entry links
            for i in range(count):
                try:
                    href = await user_links.nth(i).get_attribute("href")
                    if not href:
                        continue
                    
                    raw_key = href.split('?')[0]
                    if raw_key in seen_raw:
                        continue
                    seen_raw.add(raw_key)
                    stats["seen_links"] += 1
                    
                    # This is a user-in-group link - we need to click to get post URL
                    if f"/groups/{GROUP_ID}/user/" in href:
                        # Open in new tab to get the actual post URL
                        new_page = await context.new_page()
                        try:
                            await new_page.goto(href, wait_until="domcontentloaded", timeout=20000)
                            await asyncio.sleep(2)
                            final_url = new_page.url
                            
                            if is_valid_thread_url(final_url):
                                canon = canonicalize(final_url)
                                if canon not in discovered:
                                    discovered.add(canon)
                                    stats["thread_urls_ok"] += 1
                                    print(f"   [+] Thread: {canon[:70]}...")
                            elif is_media_viewer(final_url):
                                stats["media_viewer_seen"] += 1
                                media_queue.append(final_url)
                            else:
                                # Try to find post link on the page
                                thread_link = new_page.locator(f"a[href*='/groups/{GROUP_ID}/posts/'], a[href*='/groups/{GROUP_ID}/permalink/']")
                                if await thread_link.count() > 0:
                                    post_href = await thread_link.first.get_attribute("href")
                                    if post_href:
                                        canon = canonicalize(post_href)
                                        if canon not in discovered:
                                            discovered.add(canon)
                                            stats["thread_urls_ok"] += 1
                                            print(f"   [+] Thread (from page): {canon[:70]}...")
                                else:
                                    stats["rejected_nonthread"] += 1
                        except Exception as e:
                            stats["rejected_nonthread"] += 1
                        finally:
                            await new_page.close()
                        
                except Exception:
                    continue
            
            print(f"   Threads: {stats['thread_urls_ok']} | MediaQ: {len(media_queue)}")
            
            # Scroll
            await page.keyboard.press("End")
            await asyncio.sleep(SCROLL_DELAY)
            
        # Resolve media queue
        if media_queue:
            print(f"\n[Media Resolution] Processing {len(media_queue)} media viewers...")
            for m_url in media_queue[:20]:  # Limit to avoid timeouts
                resolved = await resolve_media_to_thread(context, m_url)
                if resolved and resolved not in discovered:
                    discovered.add(resolved)
                    stats["media_resolved"] += 1
                    stats["thread_urls_ok"] += 1
                    print(f"   [+] Resolved: {resolved[:70]}...")
                else:
                    stats["media_unresolved"] += 1
        
        await context.close()
    
    # Write output
    sorted_threads = sorted(discovered)
    with open(DISCOVERED_TXT, "w", encoding="utf-8") as f:
        for url in sorted_threads:
            f.write(url + "\n")
    
    # Final report
    print("\n" + "=" * 60)
    print("DISCOVERY COMPLETE")
    print("=" * 60)
    print(json.dumps(stats, indent=2))
    print(f"\nOutput: {DISCOVERED_TXT}")
    print(f"Total threads: {len(sorted_threads)}")

if __name__ == "__main__":
    asyncio.run(run_discovery())

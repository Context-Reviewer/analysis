import time
import random
import json
import os
import re  # <--- IMPORT ADDED
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError

# ================= CONFIGURATION =================
# Path to your Chrome user profile. 
# Ensure this folder exists and Chrome is NOT running when you start this script.
PROFILE_DIR = r"C:\dev\fb_playwright_profile" 

# Output file name
OUTPUT_FILE = "captured_data.json"

# Limits to prevent infinite loops
MAX_SCROLL_ATTEMPTS = 50
MAX_COMMENT_EXPANSIONS = 10
# =================================================

def human_delay(min_seconds=1, max_seconds=3):
    """Sleeps for a random amount of time to mimic human behavior."""
    time.sleep(random.uniform(min_seconds, max_seconds))

def expand_comments(post_locator):
    """
    Iteratively clicks 'View more comments' or 'View replies' buttons 
    within a post to load as much text as possible.
    """
    print("  Expanding comments...")
    
    # Regex to match any of these phrases (case-insensitive)
    pattern = re.compile(r"View more|replies|comments|See more", re.IGNORECASE)

    for _ in range(MAX_COMMENT_EXPANSIONS):
        # Selectors for various "View more" buttons Facebook uses
        # Note: These are text-based to be more robust against class name changes
        buttons = post_locator.locator("div[role='button'], span[role='button']").filter(
            has_text=pattern
        )
        
        count = buttons.count()
        if count == 0:
            break
            
        clicked_any = False
        # Try to click the first visible one
        for i in range(count):
            btn = buttons.nth(i)
            if btn.is_visible():
                try:
                    # Scroll into view to ensure it's clickable
                    btn.scroll_into_view_if_needed()
                    btn.click(timeout=2000)
                    human_delay(1, 2)
                    clicked_any = True
                    # Only click one per pass to allow DOM to update
                    break 
                except:
                    continue
        
        if not clicked_any:
            break

def scrape_group(page, group_url, target_user):
    print(f"Navigating to group: {group_url}")
    try:
        page.goto(group_url, wait_until="domcontentloaded")
    except Exception as e:
        print(f"Error loading group URL: {e}")
        return

    human_delay(3, 5)

    # 1. Search for the user
    print(f"Searching for user: {target_user}")
    try:
        # Click Search Icon
        # Facebook Group search icon usually has aria-label="Search"
        search_icon = page.locator("div[aria-label='Search']").first
        if search_icon.is_visible():
            search_icon.click()
        else:
            # Fallback: Try to find the search input directly
            print("  Search icon not found, looking for input...")
        
        human_delay(1, 2)

        # Type in Search Box
        # It's usually an input with aria-label="Search this group" or similar
        search_input = page.locator("input[aria-label*='Search'], input[type='search']").first
        search_input.fill(target_user)
        human_delay(1, 2)
        page.keyboard.press("Enter")
        
        print("  Waiting for search results...")
        human_delay(5, 8) # Wait for results to load

    except Exception as e:
        print(f"Search failed: {e}")
        print("Please manually perform the search in the browser window if it failed, then check the console.")
        # We continue anyway, assuming the user might have helped or the feed is loaded

    # 2. Iterate and Scrape
    collected_posts = []
    seen_posts = set()
    scroll_count = 0
    
    print("Starting scraping loop...")
    
    while scroll_count < MAX_SCROLL_ATTEMPTS:
        # Identify posts. role="article" is the standard accessible way to find posts.
        posts = page.locator("div[role='article']")
        count = posts.count()
        print(f"  Screen contains {count} post candidates (Scroll {scroll_count+1}/{MAX_SCROLL_ATTEMPTS})")
        
        new_data_found = False
        
        for i in range(count):
            post = posts.nth(i)
            
            # Quick check to avoid re-processing the exact same element handle (rough check)
            # Better: use a hash of the text preview
            try:
                preview_text = post.inner_text()[:100]
            except:
                continue
                
            if preview_text in seen_posts:
                continue
            
            # Check if this post is relevant (contains the user's name)
            # This is a heuristic. The search result feed usually only shows relevant posts,
            # but we want to be sure we are looking at the right author if possible.
            # For now, we scrape everything in the search result feed.
            
            print(f"    Processing post {len(collected_posts) + 1}...")
            
            # Expand Content
            expand_comments(post)
            
            # Extract Full Text
            full_text = post.inner_text()
            
            post_data = {
                "id": str(hash(preview_text)), # Simple hash for ID
                "scraped_at": datetime.now().isoformat(),
                "raw_text": full_text
            }
            
            collected_posts.append(post_data)
            seen_posts.add(preview_text)
            new_data_found = True

        if not new_data_found:
            print("  No new posts found on this screen.")
            
        # Scroll
        page.keyboard.press("End")
        human_delay(3, 5)
        scroll_count += 1
        
        # Save intermediate results (so we don't lose everything if it crashes)
        if len(collected_posts) > 0 and len(collected_posts) % 5 == 0:
            save_data(collected_posts)

    save_data(collected_posts)
    print("Scraping complete.")

def save_data(data):
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"  [Saved {len(data)} items to {OUTPUT_FILE}]")
    except Exception as e:
        print(f"Error saving data: {e}")

def main():
    print("=== Facebook Group Scraper ===")
    
    # Input validation
    group_url = input("Enter Group URL (e.g., https://www.facebook.com/groups/12345): ").strip()
    if not group_url:
        print("Group URL is required.")
        return
        
    target_user = input("Enter Target User Name: ").strip()
    if not target_user:
        print("Target user is required.")
        return

    print(f"\nInitializing Playwright with profile: {PROFILE_DIR}")
    print("Ensure all Chrome instances are closed before proceeding.")
    
    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=PROFILE_DIR,
                headless=False,
                viewport={"width": 1280, "height": 900},
                args=["--disable-notifications"]
            )
        except Exception as e:
            print(f"\nCRITICAL ERROR: Could not launch browser.\n{e}")
            print("\nCommon causes:")
            print("1. Chrome is already open. Close it.")
            print("2. The PROFILE_DIR path is incorrect.")
            return

        # Get the first page or create new
        page = context.pages[0] if context.pages else context.new_page()
        
        # Go to FB
        page.goto("https://www.facebook.com", wait_until="domcontentloaded")
        
        print("\n" + "="*40)
        print("ACTION REQUIRED: Log in to Facebook in the browser.")
        print("If 2FA is required, complete it now.")
        print("When you are fully logged in and ready, press Enter below.")
        print("="*40 + "\n")
        
        input("Press Enter to start scraping... ")
        
        scrape_group(page, group_url, target_user)
        
        print("\nDone. Closing browser in 5 seconds...")
        time.sleep(5)
        context.close()

if __name__ == "__main__":
    main()
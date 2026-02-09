"""
Phase 2: Extraction - Post Thread URLs -> Sean Roy Comments
Usage: python step5b_phase2_extract.py

Invariants:
- Input only /posts/ or /permalink/ URLs
- Use Container Identity model (Playwright locators only)
- NO BeautifulSoup, NO Sandwich parsing
- Dump debug on zero-extraction when hits exist
"""
import asyncio
import json
import os
import hashlib
from playwright.async_api import async_playwright

# =========================
# CONFIG
# =========================
OUTPUT_DIR = r"C:\Users\lwpar\Desktop\fb_extract_out"
DISCOVERED_TXT = os.path.join(OUTPUT_DIR, "discovered_threads.txt")
COMMENTS_JSONL = os.path.join(OUTPUT_DIR, "sean_roy_comments.jsonl")
PROFILE_DIR = r"C:\dev\fb_playwright_profile"
TARGET_AUTHOR = "Sean Roy"
GROUP_ID = "3970539883001618"

MAX_EXPAND_ROUNDS = 5
EXPAND_DELAY = 1.0

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# URL VALIDATION
# =========================
def is_valid_thread_url(url: str) -> bool:
    return f"/groups/{GROUP_ID}/posts/" in url or f"/groups/{GROUP_ID}/permalink/" in url

# =========================
# TEXT CLEANING
# =========================
def clean_payload(raw_text: str) -> str:
    """Remove UI noise from extracted comment text"""
    if not raw_text:
        return ""
    
    noise_phrases = [
        TARGET_AUTHOR, "Like", "Reply", "Share", "Send",
        "See more", "View more", "Edited", "Most relevant",
        "All comments", "Write a comment", "commented on",
        "replied to", "View post", "Shared with", "Private group",
        "Public", "Admin", "Moderator", "Author", "Top contributor"
    ]
    
    lines = raw_text.split('\n')
    cleaned = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        
        # Skip if line is pure noise
        if stripped in noise_phrases:
            continue
        
        # Skip timestamp-like patterns (rough heuristic)
        if len(stripped) < 5 and any(c.isdigit() for c in stripped):
            continue
        
        # Skip if starts with common noise
        skip = False
        for noise in noise_phrases:
            if stripped.startswith(noise) and len(stripped) < len(noise) + 10:
                skip = True
                break
        
        if not skip:
            cleaned.append(stripped)
    
    return " ".join(cleaned).strip()

# =========================
# CONTAINER IDENTITY EXTRACTION
# =========================
async def extract_comments_from_thread(page, url: str) -> tuple[int, int, list]:
    """
    Extract Sean Roy comments using Container Identity model.
    Returns: (author_hits, extracted_count, records)
    """
    records = []
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)
    except Exception as e:
        print(f"   [Nav Error] {e}")
        return 0, 0, []
    
    # Guard: Verify thread surface
    if not is_valid_thread_url(page.url):
        print(f"   [Guard Fail] Not a thread: {page.url[:60]}")
        return 0, 0, []
    
    # Expansion loop - click "View more comments", "View replies", etc.
    print("   [Expand] Running expansion loop...")
    for round_num in range(MAX_EXPAND_ROUNDS):
        expand_buttons = page.locator(
            "div[role='button']:has-text('View more comments'), "
            "div[role='button']:has-text('View'), "
            "div[role='button']:has-text('See more'), "
            "span[role='button']:has-text('View')"
        )
        
        count = await expand_buttons.count()
        if count == 0:
            break
        
        clicked = False
        for i in range(min(count, 3)):  # Click up to 3 per round
            try:
                btn = expand_buttons.nth(i)
                if await btn.is_visible():
                    await btn.click(timeout=2000)
                    clicked = True
                    await asyncio.sleep(EXPAND_DELAY)
            except:
                continue
        
        if not clicked:
            break
    
    # Find author candidates
    print("   [Extract] Finding author candidates...")
    author_locator = page.get_by_text(TARGET_AUTHOR, exact=True)
    author_hits = await author_locator.count()
    print(f"   [Extract] Author hits: {author_hits}")
    
    extracted_count = 0
    
    for i in range(author_hits):
        try:
            author_node = author_locator.nth(i)
            
            # Ascend to find container with BOTH "Like" AND "Reply"
            container = None
            current = author_node
            
            for _ in range(10):  # Max 10 levels up
                parent = current.locator("..")
                
                # Check if this parent contains footer
                try:
                    has_like = await parent.locator("text=Like").count() > 0
                    has_reply = await parent.locator("text=Reply").count() > 0
                    
                    if has_like and has_reply:
                        container = parent
                        break
                except:
                    pass
                
                current = parent
            
            if not container:
                continue
            
            # Extract payload
            raw_text = await container.inner_text()
            clean_text = clean_payload(raw_text)
            
            if not clean_text or len(clean_text) < 3:
                continue
            
            # Create record
            record = {
                "post_url": url,
                "author": TARGET_AUTHOR,
                "comment_text": clean_text,
                "timestamp": None,
                "debug_hash": hashlib.md5(raw_text.encode()).hexdigest()[:8]
            }
            
            records.append(record)
            extracted_count += 1
            
        except Exception as e:
            continue
    
    return author_hits, extracted_count, records

# =========================
# DEBUG DUMP
# =========================
async def dump_debug(page, url: str, reason: str):
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    html_path = os.path.join(OUTPUT_DIR, f"debug_extract_{url_hash}.html")
    png_path = os.path.join(OUTPUT_DIR, f"debug_extract_{url_hash}.png")
    
    try:
        await page.screenshot(path=png_path)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(await page.content())
        print(f"   [Debug Dump] {reason} -> {html_path}")
    except Exception as e:
        print(f"   [Debug Error] {e}")

# =========================
# MAIN EXTRACTION LOOP
# =========================
async def run_extraction():
    print("=" * 60)
    print("PHASE 2: EXTRACTION")
    print("=" * 60)
    
    # Load discovered threads
    if not os.path.exists(DISCOVERED_TXT):
        print(f"[ERROR] No input file: {DISCOVERED_TXT}")
        return
    
    with open(DISCOVERED_TXT, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print(f"Input: {len(urls)} thread URLs")
    
    # Stats
    total_hits = 0
    total_extracted = 0
    skipped_guard = 0
    zero_extract_dumps = 0
    
    # Clear previous output
    if os.path.exists(COMMENTS_JSONL):
        os.remove(COMMENTS_JSONL)
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = await context.new_page()
        
        for idx, url in enumerate(urls):
            print(f"\n[{idx + 1}/{len(urls)}] {url[:60]}...")
            
            if not is_valid_thread_url(url):
                print("   [Skip] Invalid thread URL")
                skipped_guard += 1
                continue
            
            hits, extracted, records = await extract_comments_from_thread(page, url)
            total_hits += hits
            total_extracted += extracted
            
            # Write records
            if records:
                with open(COMMENTS_JSONL, "a", encoding="utf-8") as f:
                    for rec in records:
                        f.write(json.dumps(rec) + "\n")
            
            # Debug dump on zero extraction with hits
            if hits > 0 and extracted == 0:
                await dump_debug(page, url, "hits_no_extract")
                zero_extract_dumps += 1
            
            print(f"   Hits: {hits} | Extracted: {extracted}")
        
        await context.close()
    
    # Final report
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total threads processed: {len(urls)}")
    print(f"Skipped (guard): {skipped_guard}")
    print(f"Total author hits: {total_hits}")
    print(f"Total comments extracted: {total_extracted}")
    print(f"Zero-extract debug dumps: {zero_extract_dumps}")
    print(f"\nOutput: {COMMENTS_JSONL}")

if __name__ == "__main__":
    asyncio.run(run_extraction())

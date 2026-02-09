import asyncio
import json
import os
import re
import argparse
import hashlib
from datetime import datetime, timezone
from playwright.async_api import async_playwright

print("===================================================")
print("   ANTIGRAVITY FACEBOOK SCRAPER FIX v5.4")
print("   - Strategy: Hybrid Entry")
print("   - Fixes: LOOP BREAKING (Strict Click Limits)")
print("===================================================")

# =========================
# CONFIG
# =========================
GROUP_ID = "3970539883001618"
SEARCH_URL = f"https://www.facebook.com/groups/{GROUP_ID}/search/?q=sean%20roy"
TARGET_AUTHOR = "Sean Roy"
TARGET_AUTHOR_ARIA = "Sean Roy"

PROFILE_DIR = r"C:\dev\fb_playwright_profile"
OUT_PATH = "fb_extract_out/sean_comments.jsonl"
DEBUG_DIR = "fb_extract_out"

# =========================
# UTILS
# =========================
def ensure_out():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def text_hash(txt):
    return hashlib.md5(txt.encode("utf-8", errors="ignore")).hexdigest()

async def dismiss_overlays(page):
    try:
        if await page.get_by_role("button", name="Allow all cookies").count():
            await page.get_by_role("button", name="Allow all cookies").click()
    except: pass

# =========================
# EXPANSION
# =========================
async def expand_everything(page, max_rounds=30):
    click_counts = {}

    expand_patterns = [
        re.compile(r"View.*replies", re.IGNORECASE),
        re.compile(r"View.*comments", re.IGNORECASE),
        re.compile(r"See more", re.IGNORECASE),
        re.compile(r"\d+\sreplies", re.IGNORECASE),
        re.compile(r"\d+\scomments", re.IGNORECASE),
        re.compile(r"View\sprevious", re.IGNORECASE),
    ]

    print(f"      [Expand] Start (max {max_rounds})...")

    # Limit total time spent expanding one post
    for round_num in range(1, max_rounds + 1):
        clicked_this_round = False
        await dismiss_overlays(page)

        # 1. Button Logic
        for pat in expand_patterns:
            btns = page.get_by_role("button", name=pat)
            count = await btns.count()
            
            # Walk backwards
            for i in range(count - 1, -1, -1):
                try:
                    b = btns.nth(i)
                    if await b.is_visible():
                        txt = (await b.inner_text()).strip()
                        if "Close" in txt: continue

                        # STUCK FIX: Track clicks per text
                        # "7 comments"
                        if txt not in click_counts: click_counts[txt] = 0
                        
                        if click_counts[txt] >= 3:
                            # We already clicked this specific text 3 times, move on
                            continue

                        await b.scroll_into_view_if_needed()
                        await b.click(timeout=800)
                        await page.wait_for_timeout(500)
                        
                        click_counts[txt] += 1
                        clicked_this_round = True
                        print(f"        -> Clicked: '{txt[:30]}' ({click_counts[txt]})")
                except: pass
        
        # 2. Text Fallback
        if not clicked_this_round:
            for txt_pat in ["View more replies", "See more", "View previous comments"]:
                try:
                    loc = page.locator(f"text={txt_pat}")
                    if await loc.count() > 0 and await loc.first.is_visible():
                        # Track these too
                        if txt_pat not in click_counts: click_counts[txt_pat] = 0
                        if click_counts[txt_pat] >= 3: continue

                        await loc.first.click(timeout=500)
                        click_counts[txt_pat] += 1
                        clicked_this_round = True
                        print(f"        -> Clicked text: '{txt_pat}'")
                except: pass

        if not clicked_this_round:
            print("      [Expand] Settled.")
            break
        
        await page.mouse.wheel(0, 300)
        await page.wait_for_timeout(300)

# =========================
# EXTRACTION
# =========================
async def extract_sean_comments(page, out_fh, source_url):
    anchors = page.locator(f"a[aria-label='{TARGET_AUTHOR_ARIA}']")
    count = await anchors.count()
    emitted = 0
    
    if count > 0:
        print(f"      [Extract] Found {count} Sean Roy nodes")

    for i in range(count):
        try:
            a = anchors.nth(i)
            container = None
            
            for level in range(1, 12):
                cand = a.locator(f"xpath=ancestor::div[{level}]")
                if await cand.count() == 0: break
                
                texts = cand.locator("div[dir='auto']")
                if await texts.count() > 0:
                     tval = await texts.first.inner_text()
                     if len(tval) > 2 and tval != TARGET_AUTHOR:
                         container = cand
                         if level >= 3: break
            
            if not container:
                container = a.locator("xpath=ancestor::div[3]")

            blocks = container.locator("div[dir='auto']")
            best_text = ""
            for j in range(await blocks.count()):
                txt = (await blocks.nth(j).inner_text()).strip()
                if not txt: continue
                if txt in [TARGET_AUTHOR, "Author", "Admin", "Top contributor", "Follow"]: continue
                if len(txt) > len(best_text):
                    best_text = txt
            
            if not best_text: continue

            rec = {
                "schema": "fb-comment-extract-v5.4",
                "captured_at": utc_now(),
                "author": TARGET_AUTHOR,
                "comment_text": best_text,
                "post_url": source_url,
            }
            out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            emitted += 1
        except: continue
    
    return emitted

# =========================
# MAIN
# =========================
async def main(headless):
    ensure_out()
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=headless,
            viewport={"width": 1280, "height": 900},
            args=["--disable-notifications"]
        )
        page = await context.new_page()

        print(f"[i] Navigating to: {SEARCH_URL}")
        await page.goto(SEARCH_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        await dismiss_overlays(page)

        processed_hashes = set()

        with open(OUT_PATH, "w", encoding="utf-8") as out_fh:
            
            for scroll_idx in range(1, 50):
                
                author_hits = page.locator(f"a[aria-label='{TARGET_AUTHOR_ARIA}']")
                count = await author_hits.count()
                print(f"[Scroll {scroll_idx}] found {count} potential anchors")
                
                for i in range(count):
                    try:
                        anchor = author_hits.nth(i)
                        if not await anchor.is_visible(): continue

                        card = None
                        click_target = None
                        
                        for level in range(3, 18):
                            cand = anchor.locator(f"xpath=ancestor::div[{level}]")
                            if await cand.count() == 0: break
                            
                            c_btn = cand.get_by_role("button", name=re.compile(r"\d+\s(comment|repl)", re.I)).first
                            if await c_btn.count() > 0:
                                card = cand
                                click_target = c_btn
                                break
                            
                            lbl_btn = cand.locator("div[aria-label='Leave a comment'], div[aria-label='Comment']")
                            if await lbl_btn.count() > 0:
                                card = cand
                                click_target = lbl_btn.first
                                break

                        if not card: 
                            cand = anchor.locator("xpath=ancestor::div[10]")
                            if await cand.count() > 0:
                                card = cand
                                click_target = cand
                        
                        if not card: continue

                        txt = await card.inner_text()
                        h = text_hash(txt)
                        if h in processed_hashes: continue
                        processed_hashes.add(h)
                        
                        print(f"   -> Opening: {txt[:40].replace(chr(10), ' ')}...")
                        
                        if click_target:
                            await click_target.scroll_into_view_if_needed()
                            await page.wait_for_timeout(300)
                            try:
                                await click_target.click(timeout=1500)
                            except:
                                await card.click(position={"x": 50, "y": 50}, timeout=1500)
                            
                        await page.wait_for_timeout(3500)
                        
                        await expand_everything(page) # Now has click limits
                        
                        n = await extract_sean_comments(page, out_fh, page.url)
                        if n > 0:
                            print(f"      + Extracted {n} items")
                        
                        await page.keyboard.press("Escape")
                        await page.wait_for_timeout(500)
                        close_btn = page.get_by_role("button", name="Close")
                        if await close_btn.count(): await close_btn.first.click()
                        await page.wait_for_timeout(500)

                    except Exception as e:
                        print(f"      Error: {e}")
                        pass
                
                await page.mouse.wheel(0, 3000)
                await page.wait_for_timeout(2000)

        print(f"[✓] Finished. Check {OUT_PATH}")
        await context.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", type=int, default=0)
    args = ap.parse_args()

    asyncio.run(main(bool(args.headless)))

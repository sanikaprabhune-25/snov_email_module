import time
import socket
import threading
import sys
import ast
import re
import pandas as pd
import random
import string
import io
import base64
from urllib.parse import urlparse
import heapq

from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit

import asyncio
import inspect
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
    Browser,          # For type hinting
    BrowserContext,
    Page,
    ElementHandle
)

# ─── 1) FLASK + SOCKETIO SETUP ─────────────────────────────────

app = Flask(__name__)
CORS(app)

seven_days = 7 * 24 * 60 * 60
socketio = SocketIO(
    app,
    async_mode="threading",      # Force Python threading in SocketIO
    cors_allowed_origins="*",
    engineio_options={'transports': ['polling']},
    ping_interval=seven_days,
    ping_timeout=seven_days
)
print("→ SocketIO async_mode =", socketio.async_mode)

# Store per‐client state here
clients_state = {}

class MySpecialError(Exception):
    """Raised when a special condition occurs or for cancellation."""
    pass

def get_domain(url):
    if not isinstance(url, str) or not url.strip():
        return ''
    raw = re.sub(r'[\x00-\x1F\x7F]', '', url).strip()
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9+.\-]*://', raw):
        raw = 'http://' + raw
    parsed = urlparse(raw)
    return parsed.netloc.replace('www.', '').strip()

async def check_cancel(state, description=""):
    if state.get("cancel_process"):
        raise MySpecialError(f"Process cancelled by user during: {description}")

# ─── 2) NETWORK MONITOR ──────────────────────────────────────────

network_up = True

def check_network() -> bool:
    """Quick TCP handshake to Google DNS; False if down."""
    try:
        sock = socket.create_connection(("8.8.8.8", 53), timeout=2)
        sock.close()
        return True
    except:
        return False

async def monitor_network():
    global network_up
    while True:
        up = check_network()
        if up and not network_up:
            print("▶️ Resumed — network up")
        if not up and network_up:
            print("⏸️ Paused — network down")
        network_up = up
        await asyncio.sleep(1)

# ─── 3) PATCH PLAYWRIGHT FOR AUTO‑PAUSE + CONTEXT RELOAD ─────────

def make_network_aware(cls):
    for name, method in inspect.getmembers(cls, inspect.iscoroutinefunction):
        orig = method
        async def wrapper(self, *args, _orig=orig, _meth=name, **kwargs):
            # wait until network is up
            while not network_up:
                await asyncio.sleep(1)
            try:
                return await _orig(self, *args, **kwargs)
            except (PlaywrightTimeoutError, PlaywrightError):
                if not check_network():
                    print(f"⏸️ Paused during {_meth}, waiting for network…")
                    while not check_network():
                        await asyncio.sleep(1)
                    print(f"▶️ Resumed — reloading context & retrying {_meth}")
                    # reload pages in this context/browser
                    targets = []
                    if isinstance(self, Page):
                        targets = [self]
                    elif isinstance(self, BrowserContext):
                        targets = self.pages
                    elif isinstance(self, Browser):
                        for ctx in self.contexts:
                            targets.extend(ctx.pages)
                    for pg in targets:
                        try: await pg.reload()
                        except: pass
                    return await _orig(self, *args, **kwargs)
                raise
        setattr(cls, name, wrapper)

make_network_aware(Browser)
make_network_aware(BrowserContext)
make_network_aware(Page)

# ─── 4) GLOBALS FOR THE PLAYWRIGHT WORKER ───────────────────────

playwright_job_queue = None
WORKER_EVENT_LOOP = None

async def playwright_worker_main():
    global playwright_job_queue, WORKER_EVENT_LOOP
    WORKER_EVENT_LOOP = asyncio.get_running_loop()

    # start background network monitor
    asyncio.create_task(monitor_network())

    pw_instance = await async_playwright().start()
    browser: Browser = await pw_instance.chromium.launch(
        headless=False,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"]
    )
    print("→ Playwright worker: browser launched.")

    playwright_job_queue = asyncio.Queue()

    while True:
        job_details = await playwright_job_queue.get()
        if job_details is None:
            break

        (sid, domains_list, designations_list, location_list2,
         converted_list, required_counts, num_results_arg, downloadFileName_arg) = job_details

        state = clients_state.get(sid)
        if state is None or state.get("cancel_process"):
            continue

        asyncio.create_task(
            handle_one_job(
                browser, pw_instance, sid,
                domains_list, designations_list, location_list2,
                converted_list, required_counts,
                num_results_arg, downloadFileName_arg
            )
        )

    print("→ Playwright worker: shutting down browser and Playwright.")
    await browser.close()
    await pw_instance.stop()
    print("→ Playwright worker: shutdown complete.")

def start_playwright_worker_thread():
    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(playwright_worker_main())
        loop.close()
    thread = threading.Thread(target=run_loop, daemon=True, name="PlaywrightWorkerThread")
    thread.start()
    print("→ Playwright worker thread started.")
    return thread

worker_thread = start_playwright_worker_thread()

# ─── 5) HANDLE_ONE_JOB (YOUR LOGIC UNCHANGED) ───────────────────

async def handle_one_job(
    browser: Browser, pw_instance, sid: str,
    domains: list, designations: list, locations: list,
    cookies: list, required_counts: dict, num_results_arg: int, downloadFileName: str
):
    state = clients_state.get(sid)
    if state is None or state.get("cancel_process"):
        if playwright_job_queue: playwright_job_queue.task_done() # Must be called if job is taken from queue
        return

    context: BrowserContext = None
    page: Page = None
    result_message = "Processed successfully."
    
    # Data accumulation lists, local to this job
    file_data = []
    file_data2 = []
    emailcount = 0
    temp_file_data2_id = 1
    processed_count = -1 # Corrected from original, should be 0 for first item
    total_domains = len(domains)
    domain_remaining = domains[:] # Make a copy to modify

    # Clear previous results for this client for this new job run
    state["df_list"].clear()
    state["preview_list"].clear()

    try:
        await check_cancel(state, "Job initialization")
        print(f"[{sid}] Creating new browser context for job...")
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
        state["current_context"] = context # For potential cancellation by refresh/disconnect
        state["current_page"] = page
        print(f"[{sid}] New browser context and page created.")

        await page.goto("https://app.snov.io/prospects/list")
        await asyncio.sleep(4) # Yield control and allow page to load
        await check_cancel(state, "Navigated to SNOV list page")

        print(f"[{sid}] Attempting to add cookies...")
        await context.clear_cookies() # Clear any residual cookies in the context
        playwright_cookies_to_add = []
        for sel_cookie_orig in cookies:
            cookie = sel_cookie_orig.copy()
            if "sameSite" in cookie and cookie["sameSite"] not in ["Strict", "Lax", "None"]:
                cookie.pop("sameSite")
            if "expirationDate" in cookie:
                if not isinstance(cookie["expirationDate"], (int, float)): # Allow float then convert
                    try:
                        cookie["expirationDate"] = float(cookie["expirationDate"])
                    except ValueError:
                        cookie.pop("expirationDate", None)
                if isinstance(cookie["expirationDate"], float): # Convert float to int
                     cookie["expirationDate"] = int(cookie["expirationDate"])


            pw_cookie = {
                "name": cookie.get("name"), "value": cookie.get("value"),
                "domain": cookie.get("domain"), "path": cookie.get("path", "/"),
                "httpOnly": bool(cookie.get("httpOnly", False)),
                "secure": bool(cookie.get("secure", False))
            }
            if "expirationDate" in cookie and cookie.get("expirationDate") is not None:
                pw_cookie["expires"] = cookie["expirationDate"]
            if "sameSite" in cookie: # Should be valid by now or removed
                pw_cookie["sameSite"] = cookie["sameSite"]
            
            if not (pw_cookie["name"] and pw_cookie["domain"]):
                continue
            playwright_cookies_to_add.append(pw_cookie)
        
        if playwright_cookies_to_add:
            try:
                await context.add_cookies(playwright_cookies_to_add)
                print(f"[{sid}] Added {len(playwright_cookies_to_add)} cookies.")
            except PlaywrightError as e:
                print(f"[{sid}] Playwright Warning: Could not add cookies: {e}")
            except Exception as e:
                print(f"[{sid}] Warning: Could not add one or more cookies (generic exception): {e}")
        
        await asyncio.sleep(0) # Yield
        await page.reload()
        print(f"[{sid}] Page reloaded after adding cookies.")
        await asyncio.sleep(15) # Increased sleep after reload, adjust as needed
        await check_cancel(state, "Added cookies and reloaded")

        await page.wait_for_selector("use", timeout=20_000, state="visible")
        use_elements_list = await page.query_selector_all("use")
        add_list_icon_btn_el = None
        for use_el_item in use_elements_list:
            href_val = (await use_el_item.get_attribute("xlink:href")) or (await use_el_item.get_attribute("href"))
            if href_val == "#list_add_icon":
                # Try to find the button ancestor more reliably
                # This xpath assumes the <use> is inside a <svg> which is inside the <button>
                add_list_icon_btn_el = await use_el_item.query_selector("xpath=ancestor::button[1]")
                if add_list_icon_btn_el:
                    break
        if not add_list_icon_btn_el: raise MySpecialError("Could not find 'add list' icon button.")
        
        try: await add_list_icon_btn_el.click(timeout=5000)
        except PlaywrightTimeoutError: await page.evaluate("el => el.click()", add_list_icon_btn_el)
        await asyncio.sleep(0.5)

        modal_title_loc = page.locator("div.modal-snovio__title", has_text="Create a new prospects list")
        await modal_title_loc.wait_for(state="visible", timeout=20_000)
        
        name_input_modal_loc = page.locator("div.modal-snovio__window input.snov-input-wrapper__input--minimized")
        await name_input_modal_loc.wait_for(state="visible", timeout=20_000)
        await name_input_modal_loc.fill(str(downloadFileName)) 
        await asyncio.sleep(0.2)

        await page.wait_for_selector("button[data-test='snov-modal-btn-primary']", state="attached", timeout=20_000)
        create_btn_modal_loc = page.locator("button[data-test='snov-modal-btn-primary']", has_text="Create")
        create_btn_el_handle = await create_btn_modal_loc.element_handle(timeout=5000) 
        if not create_btn_el_handle or not await create_btn_el_handle.is_visible():
            candidate_btns = await page.query_selector_all("button[data-test='snov-modal-btn-primary']")
            found_create_btn_handle = None
            for btn_h in candidate_btns:
                if await btn_h.is_visible() and (await btn_h.inner_text()).strip() == "Create":
                    found_create_btn_handle = btn_h; break
            if not found_create_btn_handle: raise MySpecialError("Could not find visible Create button in modal (Playwright)")
            create_btn_el_handle = found_create_btn_handle
        
        await create_btn_el_handle.scroll_into_view_if_needed()
        await page.evaluate("el => el.click()", create_btn_el_handle) # Use evaluate for tricky clicks
        await page.wait_for_selector("div.modal-snovio__window", state="hidden", timeout=10_000)
        await check_cancel(state, "Created list")

        await page.goto("https://app.snov.io/database-search/prospects")
        await asyncio.sleep(2) 
        await check_cancel(state, "Navigated to database search")
        location_input_xpath = "//div[contains(@class,'snov-filter')][.//span[text()='Location']]//input[contains(@class,'snov-filter__block-input')]"
        location_dropdown_list_xpath = "//div[contains(@class,'snov-filter')][.//span[text()='Location']]//ul[contains(@class,'snov-filter__list')]"
        location_first_option_xpath = ("(//div[contains(@class,'snov-filter')][.//span[text()='Location']]"
                                       "//ul[contains(@class,'snov-filter__list')]"
                                       "//div[contains(@class,'snov-filter__option') and .//div[contains(@class,'snov-filter__option-cnt')]][1])")
        
        if locations: 
            for loc_item_str in locations:
                await check_cancel(state, f"setting location {loc_item_str}")
                try:
                    await page.wait_for_selector(".snovContentLoader.snovContentLoader--full-screen", state="hidden", timeout=20_000)
                    loc_input_el = await page.wait_for_selector(location_input_xpath, state="visible", timeout=20_000)
                    try: await loc_input_el.click(timeout=3000)
                    except PlaywrightTimeoutError: await page.evaluate("el => el.click()", loc_input_el)
                    
                    await loc_input_el.fill(loc_item_str)
                    await page.wait_for_selector(location_dropdown_list_xpath, state="visible", timeout=20_000)
                    first_opt_el = await page.wait_for_selector(location_first_option_xpath, state="visible", timeout=20_000)
                    
                    try: await first_opt_el.click(timeout=3000)
                    except PlaywrightTimeoutError: await page.evaluate("el => el.click()", first_opt_el)
                    await asyncio.sleep(0.3) 
                except MySpecialError: raise
                except Exception as e_loc_add: print(f"[{sid}] An error occurred while setting location '{loc_item_str}': {e_loc_add}")
        await check_cancel(state, "Finished adding locations")

        company_name_input_xpath_pd = "//input[@placeholder='Enter company name']"
        first_suggestion_locator_xpath_pd = (
            "//input[@placeholder='Enter company name']"
            "/ancestor::div[contains(@class, 'snov-filter__block')]"
            "/following-sibling::ul[contains(@class, 'snov-filter__list')]/div[@data-v-f10c3200]" # Be careful with generated data-v attributes
            "/div[contains(@class, 'snov-filter__option')][1]"
        )
        search_button_xpath_pd = "//button[.//span[text()='Search']]"
        num_results_for_domain_cap = num_results_arg
    
        for domain_idx, domain_str_item in enumerate(domains):
            await check_cancel(state, f"Starting domain {domain_str_item}")
            processed_count += 1
            remaining_calc = total_domains - (processed_count + 1)
            socketio.emit(
                "overall_progress",
                {"total": total_domains, "processed": (processed_count + 1), "remaining": remaining_calc},
                room=sid,
            )
            await asyncio.sleep(0.01) # Miniscule sleep just to ensure emit goes
            print(f"[{sid}] Processing domain: {domain_str_item}")
            socketio.emit(
                "progress_update",
                {"domain": domain_str_item, "message": "Domain processing started"},
                room=sid,
            )

            num_results = num_results_for_domain_cap # Reset cap for each domain as per original logic
            found_flag_domain = False
            found2_flag_domain = False # Seems to track if a "good" email (green/yellow) was found

            company_domain_search_button_el: ElementHandle = None
            job_title_filter_input_el: ElementHandle = None

            clear_company_selector = (
                "div.snov-filter__block:has(div.snov-filter__name:has-text(\"Company name\")) "
                "span.snov-filter__block-clear"
            )
            try:
                clear_company_btn = await page.query_selector(clear_company_selector)
                if clear_company_btn and await clear_company_btn.is_visible():
                    await clear_company_btn.click(timeout=2000)
                    print(f"[{sid}] Cleared 'Company name' filter for domain {domain_str_item}.")
                    await asyncio.sleep(0.3)
            except PlaywrightTimeoutError:
                print(f"[{sid}] No 'Company name' clear button found or not clickable for domain {domain_str_item}; skipping clear.")
            except Exception as e_clear_company:
                print(f"[{sid}] Error trying to clear company name filter: {e_clear_company}")

            try:
                await page.wait_for_selector("div.db-search__filters", timeout=20_000, state="visible")
                company_name_input_el = await page.wait_for_selector(
                    company_name_input_xpath_pd, state="visible", timeout=20_000
                )
                try: await company_name_input_el.click(timeout=3000)
                except PlaywrightTimeoutError: await page.evaluate("el => el.click()", company_name_input_el)
                
                await company_name_input_el.fill(domain_str_item)
                print(f"[{sid}] Entered '{domain_str_item}' into company name input.")

                # Wait for suggestions to appear; the locator might need to be more robust
                # Snov.io might have dynamic loading, so ensure the specific suggestion is loaded
                first_sugg_el = await page.wait_for_selector(
                    first_suggestion_locator_xpath_pd, state="visible", timeout=20_000 
                )
                try: await first_sugg_el.click(timeout=30_000) # Increased timeout for click
                except PlaywrightTimeoutError: await page.evaluate("el => el.click()", first_sugg_el)
                print(f"[{sid}] Clicked first company suggestion for '{domain_str_item}'.")
                await check_cancel(state, f"Selected company suggestion for {domain_str_item}")
                await asyncio.sleep(0.5) # Allow UI to update

                company_domain_search_button_el = await page.wait_for_selector(
                    search_button_xpath_pd, state="visible", timeout=20_000
                )
                job_title_filter_input_el = await page.wait_for_selector(
                    "//div[contains(@class,'snov-filter')][.//span[text()='Job title']]"
                    "//input[contains(@class,'snov-filter__block-input')]",
                    state="visible",
                    timeout=20_000,
                )
            except MySpecialError: raise
            except Exception as e_domain_setup:
                print(f"[{sid}] Error during search setup for domain '{domain_str_item}': {e_domain_setup}")
                socketio.emit("progress_update", {"domain": domain_str_item, "message": "Domain not available in snov.io"}, room=sid)
                #------------------------------------------
                file_data2.append({
                        "First Name": str(temp_file_data2_id), "job title": "N/A", "company": "N/A",
                        "location": "N/A", "email": "Not found (domain level)", "domain": domain_str_item
                })
                temp_file_data2_id += 1
                                #-------------------------------
                for record_f2_final in file_data2:
                    state["preview_list"].append(record_f2_final)
                if state.get("preview_list"):
                    try:
                        print(f"[{sid}] Emitting preview_data with {len(state['preview_list'])} items.")
                        socketio.emit("preview_data", {"previewData": state["preview_list"]}, room=sid)
                    except Exception as e_emit_preview_final:
                        print(f"[{sid}] Error emitting final preview data: {e_emit_preview_final}")
                    #------------------------------------------
                temp_file_data2_id += 1
                #--------------------------------
                #------------------------------------------
                continue # Skip to next domain
            
            for title_idx, title_str_item in enumerate(designations):
                await check_cancel(state, f"Processing designation '{title_str_item}' for domain '{domain_str_item}'")
                socketio.emit("progress_update", {"domain": domain_str_item, "message": f"Processing designation: {title_str_item}"}, room=sid)
                await asyncio.sleep(0.01)

                if num_results <= 0:
                    print(f"[{sid}] Domain email cap (num_results={num_results}) reached for '{domain_str_item}'. Skipping designation '{title_str_item}'.")
                    break

                print(f"[{sid}] Current designation: '{title_str_item}' for domain '{domain_str_item}'")

                clear_job_title_selector = (
                    "div.snov-filter__block:has(div.snov-filter__name:has-text(\"Job title\")) "
                    "span.snov-filter__block-clear"
                )
                try:
                    clear_job_btn = await page.query_selector(clear_job_title_selector)
                    if clear_job_btn and await clear_job_btn.is_visible():
                        await clear_job_btn.click(timeout=2000)
                        print(f"[{sid}] Cleared 'Job title' filter for designation '{title_str_item}'.")
                        await asyncio.sleep(0.3)
                except PlaywrightTimeoutError:
                    print(f"[{sid}] No 'Job title' clear button found or not clickable for '{title_str_item}'; skipping clear.")
                except Exception as e_clear_job:
                    print(f"[{sid}] Error trying to clear job title filter: {e_clear_job}")

                if not job_title_filter_input_el: # Should have been found earlier
                    print(f"[{sid}] Job title input not available for '{domain_str_item}'. Breaking designation loop.")
                    break
                
                try:
                    await job_title_filter_input_el.fill(title_str_item)
                    await job_title_filter_input_el.press("Enter") # Ensure filter is applied
                    await asyncio.sleep(0.2) # Allow filter to apply

                    if not company_domain_search_button_el: # Should have been found earlier
                        print(f"[{sid}] Search button not available for '{domain_str_item}'. Breaking designation loop.")
                        break
                    await company_domain_search_button_el.click()
                    print(f"[{sid}] Search initiated for domain: {domain_str_item}, title: {title_str_item}")
                    await asyncio.sleep(0.8) # Wait for search results to begin loading
                except Exception as e_apply_filter_search:
                    print(f"[{sid}] Error applying filter or searching for title '{title_str_item}': {e_apply_filter_search}")
                    continue # Skip to next designation

                no_prospects_msg_xpath = "//div[@class='not-found__title' and normalize-space(.)=\"We couldn’t find any prospects that match your search\"]"
                try:
                    await page.wait_for_selector(no_prospects_msg_xpath, state="visible", timeout=10_000) # Adjust timeout as Snov.io can be slow
                    print(f"[{sid}] No prospects found for '{title_str_item}' in domain '{domain_str_item}'.")
                    if title_idx == len(designations) - 1 and not found_flag_domain:
                        print(f"[{sid}] Last designation ('{title_str_item}') and no email found yet for domain '{domain_str_item}' (no prospects path). Adding to file_data2.")
                        file_data2.append({
                            "First Name": str(temp_file_data2_id), "job title": "N/A", "company": "N/A",
                            "location": "N/A", "email": "Not found", "domain": domain_str_item
                        })
                        temp_file_data2_id += 1

                        #-------------------------------
                        for record_f2_final in file_data2:
                            state["preview_list"].append(record_f2_final)
                        if state.get("preview_list"):
                            try:
                                print(f"[{sid}] Emitting preview_data with {len(state['preview_list'])} items.")
                                socketio.emit("preview_data", {"previewData": state["preview_list"]}, room=sid)
                            except Exception as e_emit_preview_final:
                                print(f"[{sid}] Error emitting final preview data: {e_emit_preview_final}")
                            #------------------------------------------
                        temp_file_data2_id += 1
                        #--------------------------------



                    continue # To next designation
                except MySpecialError: raise
                except PlaywrightTimeoutError: # This is the "good" path - no "not found" message means prospects might exist
                    print(f"[{sid}] Prospects potentially found for '{title_str_item}' (no 'not found' message).")
                except Exception as e_no_prospects_check:
                    print(f"[{sid}] Error checking for 'no prospects' message: {e_no_prospects_check}")
                    continue

                try:
                    # Wait for at least one table row to ensure results are loaded
                    await page.wait_for_selector("css=tbody tr", state="attached", timeout=15_000)
                except MySpecialError: raise
                except PlaywrightTimeoutError:
                    print(f"[{sid}] Timed out waiting for table rows for title '{title_str_item}'.")
                    continue # To next designation
                
                # --- Helper: playwright_wait_all_email_cells_final (async) ---
                async def _async_wait_all_email_cells_final(pg_handle: Page, current_state, current_domain, current_title, timeout_seconds=20.0):
                    start_time_sec = time.monotonic()
                    print(f"[{sid}] Waiting for all email cells in table to stabilize for {current_domain}/{current_title}...")
                    while time.monotonic() - start_time_sec < timeout_seconds:
                        await check_cancel(current_state, f"waiting email states for {current_domain}/{current_title}")
                        all_rows_in_table = await pg_handle.query_selector_all("css=tbody tr")
                        if not all_rows_in_table:
                            await asyncio.sleep(0.5); continue
                        
                        all_cells_are_final = True
                        for r_idx, r_el_h in enumerate(all_rows_in_table):
                            try:
                                email_cell_el_h = await r_el_h.query_selector("css=td.row__cell--email")
                                if not email_cell_el_h:
                                    all_cells_are_final = False; break
                                text_in_cell = (await email_cell_el_h.inner_text()).strip()
                                is_final_state = ("@" in text_in_cell or 
                                                  "No email found" in text_in_cell or
                                                  ("Click" in text_in_cell and "Add to list" in text_in_cell) or # Less reliable
                                                  (text_in_cell == "" and await r_el_h.query_selector("css=td.row__cell--action button span:has-text('Add to list')"))) # Less reliable

                                if not is_final_state:
                                    all_cells_are_final = False; break
                            except Exception as e_cell_check:
                                print(f"[{sid}]   Row {r_idx}: Error checking email cell: {e_cell_check}")
                                all_cells_are_final = False; break
                        
                        if all_cells_are_final:
                            print(f"[{sid}] All email cells appear to be in a final state for {current_domain}/{current_title}.")
                            return True
                        await asyncio.sleep(0.5)
                    print(f"[{sid}] Timed out waiting for all email cells to stabilize for {current_domain}/{current_title}.")
                    return False

                if not await _async_wait_all_email_cells_final(page, state, domain_str_item, title_str_item):
                    print(f"[{sid}] Skipping title '{title_str_item}' due to unstable email cells.")
                    continue # To next designation
                
                print(f"[{sid}] --- Extracting prospects for heap processing (Title: '{title_str_item}') ---")
                heap_processing_rows = await page.query_selector_all("css=tbody tr.row, tbody tr[data-v-e59f4168]") # data-v attribute is risky
                if not heap_processing_rows:
                     heap_processing_rows = await page.query_selector_all("css=tbody tr")

                if not heap_processing_rows:
                    print(f"[{sid}] No rows found for heap processing for title '{title_str_item}'.")
                    continue

                max_heap = [] # Stores (priority, unique_id, element_handle)
                heap_id_counter = 0
                # heap_priority_keywords = { 
                #     "head": -7, "manager": -6, "vice president": -5, "vp": -5,
                #     "director": -4, "general manager": -3, "engineer": -2
                # }
                heap_priority_keywords = { 
                    "sr head": -17, "senior head": -16, "head": -15, "sr manager": -14,
                    "senior manager": -13, "manager": -12, "deputy manager": -11,"asst manager": -10,"assistant manager": -9,
                    "vice president":-8,"vp":-7,"president":-6,"director":-5,"general manager":-4 , "deputy general manager":-3 , 
                    "engineer":-2
                }
                # This 'used_priority_keywords' logic per row in original code might be flawed if it's meant per title
                # For now, replicating original logic:
                ##################r_h_idx is index starting from 0
                ##################r_h_item is row element tr of that row.
                for r_h_idx, r_h_item in enumerate(heap_processing_rows):
                    await check_cancel(state, f"Building heap for {domain_str_item}/{title_str_item}, row {r_h_idx}")
                    try:
                        job_title_div_el = await r_h_item.query_selector("css=td.row__cell--name > div")
                        designation_text_lower = (await job_title_div_el.inner_text()).strip().lower() if job_title_div_el else ""
                        
                        p_val = 0 
                        used_priority_keywords_for_row = set() # Reset for each row as per original implied logic
                        for keyword, priority in heap_priority_keywords.items():
                            if keyword in designation_text_lower and keyword not in used_priority_keywords_for_row:
                                p_val = priority
                                used_priority_keywords_for_row.add(keyword) # Add to row-specific set
                                break # Take first match
                        
                        heap_id_counter += 1
                        heapq.heappush(max_heap, (p_val, heap_id_counter, r_h_item)) # Add unique id for stable sort
                        # heapq.heappush(max_heap, (p_val, r_h_item)) 
                    except Exception as e_heap_build:
                        print(f"[{sid}] Error adding to heap for row {r_h_idx}: {e_heap_build}")

                num_of_emails_needed_for_title = required_counts.get(title_str_item.lower(), float('inf')) # Ensure title_str_item is lowercased if keys in required_counts are
                heap_attempt_count = 0
                # priority_email_found_in_heap = False

                # --- Helper: playwright_wait_stable_email_text_in_cell (async) ---
                async def _async_wait_stable_email_text_in_cell(email_cell_h: ElementHandle, current_state, current_domain, current_title, timeout_s=45, poll_s=0.5):
                    end_t = time.monotonic() + timeout_s; last_t = None; stable_st = None; conf_t = 1.0
                    while time.monotonic() < end_t:
                        await check_cancel(current_state, f"stable email text {current_domain}/{current_title}")
                        curr_t = None
                        try:
                            if not email_cell_h or not await email_cell_h.is_visible(): # Basic check
                                await asyncio.sleep(poll_s); continue
                            curr_t = (await email_cell_h.inner_text()).strip()
                        except Exception as e_gettext: # Catches if element becomes stale
                            await asyncio.sleep(poll_s); continue 
                        
                        is_valid_fmt = "@" in curr_t and "." in curr_t and "No email found" not in curr_t
                        if is_valid_fmt:
                            if curr_t == last_t:
                                if stable_st is None: stable_st = time.monotonic()
                                elif time.monotonic() - stable_st >= conf_t:
                                    return curr_t # Stabilized valid email
                            else: last_t = curr_t; stable_st = None
                        elif curr_t == "No email found":
                            return curr_t # Stabilized "No email found"
                        elif curr_t == "" and await email_cell_h.query_selector("xpath=ancestor::tr//button[.//span[text()='Add to list']]"):
                             return curr_t # Stabilized empty cell with "Add to list" button elsewhere (indicates action needed)
                        else: # Other text, or not yet stable
                            last_t = curr_t; stable_st = None
                        await asyncio.sleep(poll_s)
                    raise PlaywrightTimeoutError(f"Email text did not stabilize in cell. Last: '{last_t}'")

                # --- Helper: playwright_wait_button_status_saved (async) ---
                async def _async_wait_button_status_saved(row_el_h: ElementHandle, current_state, current_domain, current_title, timeout_s=15, poll_s=0.5):
                    end_t = time.monotonic() + timeout_s
                    while time.monotonic() < end_t:
                        await check_cancel(current_state, f"btn saved status {current_domain}/{current_title}")
                        # Check if row element is still valid
                        try:
                            if not row_el_h or not await row_el_h.is_visible(): # Basic check
                                await asyncio.sleep(poll_s); continue
                        except Exception: # Stale element
                            raise PlaywrightTimeoutError("Row element became stale while waiting for 'Saved' status.")

                        span_el = await row_el_h.query_selector('css=td.row__cell--action .add-list__btn span, td.row__cell--action button[disabled] span')
                        if span_el:
                            try:
                                current_stat_txt = (await span_el.inner_text()).strip()
                                if current_stat_txt == "Saved":
                                    return "Saved"
                            except Exception: # span might have become stale
                                pass
                        await asyncio.sleep(poll_s)
                    raise PlaywrightTimeoutError("Button status did not become 'Saved'.")

                # --- Helper: playwright_check_tick_mark_in_dropdown_item (async) ---
                async def _async_check_tick_mark_in_dropdown_item(dd_item_el_h: ElementHandle):
                    # Ensure dd_item_el_h is valid before querying
                    if not dd_item_el_h: return False
                    try:
                        if not await dd_item_el_h.is_visible(): return False
                    except Exception: return False # Stale

                    use_tag_el = await dd_item_el_h.query_selector("css=svg.arrow-icon-selected use, svg.snov-dropdown__check-icon use")
                    if use_tag_el:
                        try:
                            href_attr = (await use_tag_el.get_attribute("xlink:href")) or (await use_tag_el.get_attribute("href"))
                            if href_attr and ("#check-dropdown_icon" in href_attr or "#check_snov_dropdown" in href_attr):
                                return True
                        except Exception: # use_tag_el might be stale
                            return False
                    return False
                
                print(f"[{sid}] Starting final extraction from heap for title '{title_str_item}'. Need {num_of_emails_needed_for_title}. Domain cap (num_results) left: {num_results}. Heap size: {len(max_heap)}")
                emails_collected_for_this_title_run = 0
                blackandredcount=0
                
                while max_heap and emails_collected_for_this_title_run < num_of_emails_needed_for_title and num_results > 0:
                    await check_cancel(state, f"Heap processing loop for {domain_str_item}/{title_str_item}")
                    # heap_attempt_count += 1
                    # if not priority_email_found_in_heap and max_heap[0][0] == 0 : # Only break if no priority items left and 10 non-priority attempts
                    #     print(f"[{sid}] Breaking heap for title '{title_str_item}': 10 non-priority attempts, no high-priority email found yet.")
                    #     break
                    if blackandredcount>=5:
                        break
                    current_p_val, _, current_row_el_handle = heapq.heappop(max_heap)
                    
                    # Check if element is still usable (basic visibility check)
                    try:
                        if not current_row_el_handle or not await current_row_el_handle.is_visible():
                            print(f"[{sid}]   Row from heap is no longer visible/attached. Skipping.")
                            continue
                    except Exception: # StaleElementException or similar
                        print(f"[{sid}]   Row from heap became stale. Skipping.")
                        continue

                    print(f"[{sid}]\n  Processing Row (from heap, priority: {current_p_val})")
                    #This final_email_str_from_row contains green or yellow email string.
                    final_email_str_from_row = None
                    name_from_row, jobtitle_from_row, company_from_row, location_from_row = "N/A", "N/A", "N/A", "N/A"
                    is_green_or_yellow_email_row = False

                    try:
                        email_cell_el_h_current = await current_row_el_handle.query_selector("css=td.row__cell--email")
                        if not email_cell_el_h_current or not await email_cell_el_h_current.is_visible():
                            print(f"[{sid}]   Email cell missing or not visible. Skipping row.")
                            continue

                        initial_email_cell_text = (await email_cell_el_h_current.inner_text()).strip()
                        # More robust check for green/yellow status
                        gy_selector = "css=span.email-status-1, span.email-status-2"
                        is_green_or_yellow_email_row = bool(await current_row_el_handle.query_selector(gy_selector))
                        # if not is_green_or_yellow_email_row and "Add to list" not in initial_email_cell_text:
                        #    blackandredcount+=1
                        # if is_green_or_yellow_email_row and "@" in initial_email_cell_text and "." in initial_email_cell_text \
                        #    and "Add to list" not in initial_email_cell_text and "No email found" not in initial_email_cell_text: # Direct email available
                        if is_green_or_yellow_email_row and "@" in initial_email_cell_text and "." in initial_email_cell_text \
                           and "Add to list" not in initial_email_cell_text and "No email found" not in initial_email_cell_text:
                            
                            potential_email_parsed = initial_email_cell_text.split()[-1] if initial_email_cell_text.split() else initial_email_cell_text
                            if "@" in potential_email_parsed and "." in potential_email_parsed:
                                final_email_str_from_row = potential_email_parsed
                                print(f"[{sid}]   Direct G/Y email found: {final_email_str_from_row}")

                                # Check if already saved to the target list
                                action_cell_el_h = await current_row_el_handle.query_selector("css=td.row__cell--action")
                                saved_btn_el_h = None
                                if action_cell_el_h:
                                    saved_btn_el_h = await action_cell_el_h.query_selector("xpath=.//button[contains(@class, 'add-list__btn')][normalize-space(.)='Saved' or .//span[normalize-space(.)='Saved']]")

                                if saved_btn_el_h:
                                    try: 
                                        await page.evaluate("el => el.click()", saved_btn_el_h) # Click 'Saved' to open dropdown
                                        await asyncio.sleep(0.7) # Wait for dropdown
                                        
                                        # Look for dropdown items (Snov.io specific selectors)
                                        dropdown_items_list_h = await page.query_selector_all("css=div.app-dropdown__drop-item[data-test='save-to-list'], div.snov-dropdown-item")
                                        is_already_ticked_in_list = False
                                        
                                        if dropdown_items_list_h:
                                            for dd_item_h in dropdown_items_list_h:
                                                if (await dd_item_h.inner_text()).strip() == downloadFileName:
                                                    if await _async_check_tick_mark_in_dropdown_item(dd_item_h):
                                                        is_already_ticked_in_list = True
                                                        print(f"[{sid}]     Email already saved in target list '{downloadFileName}'.")
                                                        socketio.emit("progress_update", {"domain": domain_str_item, "message": f"EMAIL FOUND (ALREADY IN LIST) - {final_email_str_from_row} for {title_str_item}"}, room=sid)
                                                        final_email_str_from_row = None # Nullify to prevent re-adding
                                                    else: # Not ticked, so tick it
                                                        print(f"[{sid}]     Saving to target list '{downloadFileName}' (from Saved dropdown).")
                                                        await page.evaluate("el => el.click()", dd_item_h)
                                                        await asyncio.sleep(0.3)
                                                        # Wait for toast confirmation (optional but good)
                                                        try:
                                                            toast_selector = "xpath=//div[contains(text(), 'prospects saved') or contains(text(), 'Prospects have been saved')]"
                                                            await page.wait_for_selector(toast_selector, state="visible", timeout=7_000)
                                                            await page.wait_for_selector(toast_selector, state="hidden", timeout=7_000)
                                                        except PlaywrightTimeoutError: print(f"[{sid}]     'Prospects saved' toast confirmation timeout.")
                                                    break # Found target list item
                                            if not is_already_ticked_in_list and final_email_str_from_row is None and not any((await item.inner_text()).strip() == downloadFileName for item in dropdown_items_list_h):
                                                 print(f"[{sid}]     ERROR: Target list '{downloadFileName}' NOT FOUND in dropdown (direct G/Y path).")
                                            elif final_email_str_from_row and not any((await item.inner_text()).strip() == downloadFileName for item in dropdown_items_list_h):
                                                print(f"[{sid}]     WARN: Email found, but target list '{downloadFileName}' not in dropdown. Not saving to list.")


                                        else: print(f"[{sid}]     ERROR: Dropdown for list selection not found (direct G/Y path).")
                                        await page.keyboard.press("Escape") # Close dropdown
                                        await asyncio.sleep(0.2)
                                    except Exception as e_click_saved_dd:
                                        print(f"[{sid}]     Error interacting with 'Saved' button dropdown: {e_click_saved_dd}")
                                        if await page.query_selector("div.app-dropdown__drop, div.snov-dropdown__options"): # If dropdown is open
                                            await page.keyboard.press("Escape")
                                else: # G/Y email but no "Saved" button (might be "Add to list" if not yet processed)
                                    print(f"[{sid}]   G/Y email, but no 'Saved' button. State: '{await action_cell_el_h.inner_text() if action_cell_el_h else 'N/A'}'. Will attempt 'Add to list' if necessary.")
                                    final_email_str_from_row = None # Force "Add to list" path if it's not explicitly saved

                        if not final_email_str_from_row: # Needs "Add to list" or email wasn't G/Y
                            print(f"[{sid}]   Email not G/Y or not directly available ('{initial_email_cell_text}'). Attempting 'Add to list'...")
                            action_cell_el_h = await current_row_el_handle.query_selector("css=td.row__cell--action")
                            add_list_btn_el_h = None
                            if action_cell_el_h:
                                # More specific selector for "Add to list" button that is not disabled
                                add_list_btn_el_h = await action_cell_el_h.query_selector("xpath=.//button[not(@disabled) and contains(@class, 'add-list__btn')][normalize-space(.)='Add to list' or .//span[normalize-space(.)='Add to list']]")
                            
                            if add_list_btn_el_h:
                                print(f"[{sid}]     'Add to list' button identified.")
                                try:
                                    await page.evaluate("el => el.click()", add_list_btn_el_h)
                                    await asyncio.sleep(0.7) # Wait for dropdown
                                    dropdown_items_list_h_add = await page.query_selector_all("css=div.app-dropdown__drop-item[data-test='save-to-list'], div.snov-dropdown-item")
                                    clicked_target_list_dd_item_add = False
                                    
                                    if dropdown_items_list_h_add:
                                        for dd_item_h_add in dropdown_items_list_h_add:
                                            if (await dd_item_h_add.inner_text()).strip().lower() == downloadFileName.strip().lower():
                                                print(f"[{sid}]     Found target list '{downloadFileName}' in dropdown. Clicking.")
                                                await page.evaluate("el => el.click()", dd_item_h_add)
                                                clicked_target_list_dd_item_add = True
                                                await asyncio.sleep(0.2)
                                                break
                                        if not clicked_target_list_dd_item_add:
                                            print(f"[{sid}]     ERROR: Target list '{downloadFileName}' not found in dropdown ('Add to list' path).")
                                    else: print(f"[{sid}]     ERROR: Dropdown for list selection not found ('Add to list' path).")

                                    if clicked_target_list_dd_item_add:
                                        # Wait for button to change to "Saved" and email to appear
                                        saved_status_text = await _async_wait_button_status_saved(current_row_el_handle, state, domain_str_item, title_str_item)
                                        stable_email_text_after_add = await _async_wait_stable_email_text_in_cell(email_cell_el_h_current, state, domain_str_item, title_str_item)
                                        print(f"[{sid}]     Status='{saved_status_text}', Final Email Text='{stable_email_text_after_add}'")
                                        
                                        is_green_or_yellow_email_row = bool(await current_row_el_handle.query_selector(gy_selector)) # Re-check status
                                        if is_green_or_yellow_email_row and "@" in stable_email_text_after_add and "." in stable_email_text_after_add \
                                           and "No email found" not in stable_email_text_after_add:
                                            final_email_str_from_row = stable_email_text_after_add
                                            print(f"[{sid}]     Email successfully retrieved via 'Add to list': {final_email_str_from_row}")
                                        else:
                                            print(f"[{sid}]     Email after 'Add to list' ('{stable_email_text_after_add}') not valid, not G/Y, or 'No email found'.")
                                    else: # Target list not clicked
                                        if await page.query_selector("div.app-dropdown__drop, div.snov-dropdown__options"): # If dropdown is open
                                            await page.keyboard.press("Escape")

                                except Exception as e_add_to_list_flow:
                                    print(f"[{sid}]     Error during 'Add to list' flow: {e_add_to_list_flow}")
                                    if await page.query_selector("div.app-dropdown__drop, div.snov-dropdown__options"): # If dropdown is open
                                        await page.keyboard.press("Escape")
                            elif action_cell_el_h and "Saved" in (await action_cell_el_h.inner_text()):
                                # This case means it was 'Saved' but email wasn't initially G/Y. We might need to fetch it.
                                # The _async_wait_stable_email_text_in_cell might already cover this if it was just slow to load.
                                # If it's still not G/Y after waiting, it's likely a bad/unverified email.
                                stable_email_text_if_saved = await _async_wait_stable_email_text_in_cell(email_cell_el_h_current, state, domain_str_item, title_str_item, timeout_s=10) # Shorter timeout
                                is_green_or_yellow_email_row = bool(await current_row_el_handle.query_selector(gy_selector))
                                if is_green_or_yellow_email_row and "@" in stable_email_text_if_saved and "." in stable_email_text_if_saved \
                                   and "No email found" not in stable_email_text_if_saved:
                                    final_email_str_from_row = stable_email_text_if_saved
                                    print(f"[{sid}]     Email confirmed after re-checking 'Saved' row: {final_email_str_from_row}")
                                else:
                                    print(f"[{sid}]   Button was 'Saved', but email text ('{stable_email_text_if_saved}') not G/Y or not found. Likely unverified or no email.")
                            else:
                                print(f"[{sid}]   No 'Add to list' button found, or other state. Action cell: '{await action_cell_el_h.inner_text() if action_cell_el_h else 'N/A'}'")

                    except MySpecialError: raise
                    except PlaywrightTimeoutError as pte_heap_row_proc:
                        print(f"[{sid}]     Timeout processing heap row: {pte_heap_row_proc}")
                    except Exception as e_heap_row_proc: 
                        print(f"[{sid}]     MAJOR ERROR processing heap row: {e_heap_row_proc}")
                        # import traceback; traceback.print_exc() # For debugging
                    #keep track of for a particular desgination how many black and red emails found.    
                    if final_email_str_from_row and "@" in final_email_str_from_row:
                        num_results -= 1
                        emailcount += 1
                        found_flag_domain = True # At least one email found for this domain

                        if is_green_or_yellow_email_row : # If this if statement is not also there then also it is okay.
                            found2_flag_domain = True
                            # if current_p_val < 0: # Negative priority means high priority
                            #     priority_email_found_in_heap = True
                        
                        emails_collected_for_this_title_run += 1
                        
                        # try: # Extract other details
                        #     name_el_h_val = await current_row_el_handle.query_selector("css=td.row__cell--name a")
                        #     if name_el_h_val: name_from_row = (await name_el_h_val.inner_text()).strip()
                        #     else: # Fallback if 'a' tag not present
                        #         name_cell_el_h_val = await current_row_el_handle.query_selector("css=td.row__cell--name")
                        #         if name_cell_el_h_val: name_from_row = (await name_cell_el_h_val.inner_text()).split('\n')[0].strip()
                            
                        #     jobtitle_el_h_val = await current_row_el_handle.query_selector("css=td.row__cell--name > div")
                        #     if jobtitle_el_h_val: jobtitle_from_row = (await jobtitle_el_h_val.inner_text()).strip()
                            
                        #     company_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-name a")
                        #     if company_el_h_val: company_from_row = (await company_el_h_val.inner_text()).strip()
                        #     else:
                        #         company_cell_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-name")
                        #         if company_cell_el_h_val: company_from_row = (await company_cell_el_h_val.inner_text()).strip().split('\n')[0].strip()

                        #     location_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-country")
                        #     if location_el_h_val: location_from_row = (await location_el_h_val.inner_text()).strip()
                        # except Exception as e_details_extract:
                        #     print(f"[{sid}]     Error extracting details for row: {e_details_extract}")

                        # file_data.append({
                        #     "First Name": name_from_row, "job title": jobtitle_from_row,
                        #     "company": company_from_row, "location": location_from_row,
                        #     "email": final_email_str_from_row, "domain": domain_str_item
                        # })
                        socketio.emit("progress_update", {"domain": domain_str_item, "message": f"EMAIL FOUND - {final_email_str_from_row} for {title_str_item}"}, room=sid)
                        print(f"[{sid}]   Record added. Email count for '{title_str_item}': {emails_collected_for_this_title_run}/{num_of_emails_needed_for_title}. Domain cap (num_results) left: {num_results}")
                    else:
                        blackandredcount+=1
                        print(f"[{sid}]   No valid G/Y email found for this row after all attempts. Skipping record add.")
                        # Logic for file_data2 if last designation and no G/Y email from heap
                        if title_idx == len(designations) - 1 and not found2_flag_domain: # Check found2_flag_domain for G/Y specifically
                             # Check if this domain already has a "Not found" entry from this path
                            if not any(fd2_item['domain'] == domain_str_item and "Not found (heap path)" in fd2_item['email'] for fd2_item in file_data2):
                                print(f"[{sid}]     Heap: Last designation, no G/Y email from heap yet for domain '{domain_str_item}'. Adding to file_data2.")
                                file_data2.append({
                                    "First Name": str(temp_file_data2_id), "job title": "N/A", "company": "N/A",
                                    "location": "N/A", "email": "Not found (heap path)", "domain": domain_str_item
                                })
                                #------------------------------------------
                            for record_f2_final in file_data2:
                                state["preview_list"].append(record_f2_final)
                            if state.get("preview_list"):
                                try:
                                    print(f"[{sid}] Emitting preview_data with {len(state['preview_list'])} items.")
                                    socketio.emit("preview_data", {"previewData": state["preview_list"]}, room=sid)
                                except Exception as e_emit_preview_final:
                                    print(f"[{sid}] Error emitting final preview data: {e_emit_preview_final}")
                                #------------------------------------------
                                temp_file_data2_id += 1
                
                await check_cancel(state, f"Finished heap for {domain_str_item}/{title_str_item}")
                print(f"[{sid}]\nFinished processing rows for title '{title_str_item}'. Total G/Y emails for this title run: {emails_collected_for_this_title_run}")
            
          
            if not found_flag_domain: # If NO email of any kind was found for this domain
                 print(f"[{sid}] No emails found for domain '{domain_str_item}' after all designations. Adding to file_data2.")
                 if not any(fd2_item['domain'] == domain_str_item and "Not found" in fd2_item['email'] for fd2_item in file_data2): # Avoid duplicates
                    file_data2.append({
                        "First Name": str(temp_file_data2_id), "job title": "N/A", "company": "N/A",
                        "location": "N/A", "email": "Not found (domain level)", "domain": domain_str_item
                    })
                        #------------------------------------------
                    for record_f2_final in file_data2:
                        state["preview_list"].append(record_f2_final)
                    if state.get("preview_list"):
                        try:
                            print(f"[{sid}] Emitting preview_data with {len(state['preview_list'])} items.")
                            socketio.emit("preview_data", {"previewData": state["preview_list"]}, room=sid)
                        except Exception as e_emit_preview_final:
                            print(f"[{sid}] Error emitting final preview data: {e_emit_preview_final}")
                        #------------------------------------------
                    temp_file_data2_id += 1
            
            socketio.emit("progress_update", {"domain": domain_str_item, "message": "Domain processing finished."}, room=sid)
            # Remove successfully processed domain from 'domain_remaining'
            if domain_str_item in domain_remaining:
                try:
                    domain_remaining.remove(domain_str_item)
                    print(f"[{sid}] Removed processed domain '{domain_str_item}' from remaining list.")
                except ValueError:
                    pass # Should not happen if logic is correct
            await asyncio.sleep(0.01)

        # After all domains are processed
        # The original logic for `domain_remaining` regarding `emailcount > 0` was a bit confusing.
        # It's simpler to remove domains as they are processed.
        # `domain_remaining` will now correctly hold only domains that were skipped or errored before full processing.

        if file_data: # Populated with successful G/Y email records
            df_successful_data = pd.DataFrame(file_data)
            state["df_list"].append(df_successful_data) # Storing in client's state
            print(f"[{sid}] Added {len(file_data)} successful records to state['df_list'].")

        if emailcount == 0 and domains: # If no emails were found in the entire run for any domain
            last_processed_domain_for_emit = domains[-1] if domains else "N/A"
            socketio.emit("progress_update", {"domain": last_processed_domain_for_emit, "message": "No emails found (overall process check)"}, room=sid)
            print(f"[{sid}] Overall process check: No emails found in this run.")


    except MySpecialError as e_ms:
        result_message = f"Process cancelled or special error: {str(e_ms)}"
        print(f"[{sid}] MySpecialError in handle_one_job: {e_ms}")
        socketio.emit("process_data_response", {"message": result_message, "error": "CancelledOrError"}, room=sid)
    except PlaywrightTimeoutError as pte:
        result_message = f"A timeout occurred during Playwright operation: {str(pte)}"
        print(f"[{sid}] !!! PlaywrightTimeoutError in handle_one_job: {pte}")
        socketio.emit("process_data_response", {"message": result_message, "error": "PlaywrightTimeout"}, room=sid)
    except PlaywrightError as pe:
        result_message = f"A Playwright error occurred: {str(pe)}"
        print(f"[{sid}] !!! PlaywrightError in handle_one_job: {pe}")
        socketio.emit("process_data_response", {"message": result_message, "error": "PlaywrightError"}, room=sid)
    except Exception as e_global:
        result_message = f"An unexpected error occurred: {str(e_global)}"
        import traceback
        traceback.print_exc()
        print(f"[{sid}] !!! An unexpected generic error occurred in handle_one_job: {e_global}")
        socketio.emit("process_data_response", {"message": result_message, "error": "UnexpectedError"}, room=sid)
    finally:
        print(f"[{sid}] Executing finally block for job...")
        

        
        # Add successful data to preview_list if not already there (though file_data2 should be distinct)
        # for record_ok_final in file_data:
        #     # Check by email and domain to avoid adding if somehow duplicated (unlikely with this logic)
        #     if not any(ex.get('email') == record_ok_final.get('email') and ex.get('domain') == record_ok_final.get('domain') for ex in state["preview_list"]):
        #         state["preview_list"].append(record_ok_final)
        

        # Combine file_data (successful) and file_data2 (not found) for preview
        # state["preview_list"] was cleared at the start of the job.
        # for record_f2_final in file_data2:
        #     state["preview_list"].append(record_f2_final)
        # if state.get("preview_list"):
        #     try:
        #         print(f"[{sid}] Emitting preview_data with {len(state['preview_list'])} items.")
        #         socketio.emit("preview_data", {"previewData": state["preview_list"]}, room=sid)
        #     except Exception as e_emit_preview_final:
        #         print(f"[{sid}] Error emitting final preview data: {e_emit_preview_final}")

        
        if context:
            try:
                print(f"[{sid}] Closing browser context...")
                await context.close()
                print(f"[{sid}] Browser context closed successfully.")
            except Exception as e_context_close:
                print(f"[{sid}] Error closing browser context: {e_context_close}")
        
        state["current_page"] = None
        state["current_context"] = None

        # Add remaining (unprocessed/skipped) domains to df_list in state
        df_rem_data_final_style = []
        for d_rem_item_style in domain_remaining: # domain_remaining now only has skipped domains
            df_rem_data_final_style.append({"First Name": d_rem_item_style, "email": "Skipped/Not Processed", "domain": d_rem_item_style}) # Add more info
        
        if df_rem_data_final_style:
            df_remaining_obj_final = pd.DataFrame(df_rem_data_final_style)
            state["df_list"].append(df_remaining_obj_final)
            print(f"[{sid}] Added {len(df_rem_data_final_style)} remaining/skipped domains to state['df_list'].")
        
        # Final response after everything
        # The original 'process_data_response' only sent a message.
        # If it's not an error already emitted, send a success message.
        if "CancelledOrError" not in result_message and "PlaywrightTimeout" not in result_message and "PlaywrightError" not in result_message and "UnexpectedError" not in result_message:
             if not file_data and not file_data2 : # if no data was processed at all.
                result_message = "No prospects data found or processed based on criteria."
                socketio.emit('process_data_response', {
                    "message": result_message,
                    "error": "NoDataFound", # Custom error type
                }, room=sid)
             else:
                socketio.emit("process_data_response", {"message": result_message, "status": "completed"}, room=sid)


        print(f"[{sid}] Job handle_one_job finished. Result message: {result_message}")
        if playwright_job_queue: playwright_job_queue.task_done() # Crucial: Signal completion to the queue

# ─── 6) SOCKETIO EVENT HANDLERS & RUN ───────────────────────────

@app.route("/")
def index():
    return "Flask-SocketIO + Async Playwright Worker is UP."

@socketio.on("connect")
def on_connect():
    sid = request.sid
    clients_state[sid] = {
        "df_list": [], "preview_list": [],
        "cancel_process": False,
        "current_context": None,
        "current_page": None
    }
    print(f"Client connected: {sid}")

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    state = clients_state.pop(sid, None)
    if state:
        state["cancel_process"] = True
    print(f"Client disconnected: {sid}")

@socketio.on("process_data")
def handle_process_data(payload):
    sid = request.sid
    state = clients_state.get(sid)
    if not state:
        emit("process_data_response", {"message": "State not found", "error": "StateNotFound"}, room=sid)
        return

    state["cancel_process"] = False
    state["df_list"].clear()
    state["preview_list"].clear()
    state["current_context"] = None
    state["current_page"] = None

    domains_str = payload.get('domains', '')
    domains_list = [get_domain(d) for d in domains_str.splitlines() if d.strip()]

    designations_payload = payload.get('designations', '')
    downloadFileName_arg = payload.get('downloadFileName', 'Scraped_Prospects_List')
    designations_list = [d.strip() for d in designations_payload.split(',') if d.strip()]

    location_payload = payload.get('location', '')
    location_list2 = [l.strip().lower() for l in location_payload.split(',') if l.strip()]

    array_data_str = payload.get('arrayData', '[]')
    num_results_arg = int(payload.get('numResults', 10))
    try:
        cookies = ast.literal_eval(array_data_str.replace("true", "True").replace("false", "False"))
        if not isinstance(cookies, list):
            raise ValueError
    except Exception as e:
        emit("process_data_response", {"message": f"Invalid cookie data: {e}", "error": "CookieParsingFailed"}, room=sid)
        return

    required_counts = {}
    parsed_designations = []
    pattern = re.compile(r'^\s*(.+?)\s*(\d+)\s*$')
    for item in designations_list:
        m = pattern.search(item)
        if m:
            name = m.group(1).lower().strip()
            cnt = int(m.group(2))
            parsed_designations.append(name)
            required_counts[name] = cnt
        else:
            parsed_designations.append(item.lower())

    print(f"[{sid}] Job to queue: Domains={len(domains_list)}, Designations={parsed_designations}, Locations={location_list2}, Cookies={len(cookies)}")
    print(f"[{sid}] Required counts: {required_counts}")

    job = (
        sid,
        domains_list,
        parsed_designations,
        location_list2,
        cookies,
        required_counts,
        num_results_arg,
        downloadFileName_arg
    )
    if not WORKER_EVENT_LOOP or not playwright_job_queue:
        emit("process_data_response", {"message": "Worker not ready", "error": "WorkerNotReady"}, room=sid)
        return

    WORKER_EVENT_LOOP.call_soon_threadsafe(playwright_job_queue.put_nowait, job)
    emit("process_data_response", {"message": "Playwright job queued successfully."}, room=sid)

@socketio.on("refresh")
def handle_refresh():
    sid = request.sid
    state = clients_state.get(sid)
    if state:
        state["cancel_process"] = True
    emit("refresh_response", {"message": "Application state refreshed. Ongoing process will be cancelled."}, room=sid)

if __name__ == "__main__":
    socketio.run(
        app,
        debug=True,  # Set to False in production
        host="0.0.0.0",
        port=5000,
        allow_unsafe_werkzeug=True,
        use_reloader=False
    )

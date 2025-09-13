import asyncio
import threading
import ast
import re
import pandas as pd
import time
import random
import string
import io
import base64
from urllib.parse import urlparse
import heapq
from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# --- Playwright ASYNC Imports ---
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
    Browser,
    BrowserContext,
    Page,
    ElementHandle
)

# FLASK + SOCKETIO SETUP 
app = Flask(__name__)
CORS(app)

socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins="*"
)

print("SocketIO async_mode =", socketio.async_mode)

# Store per-client state
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
    domain = parsed.netloc.replace('www.', '').strip()
    return domain

async def check_cancel(state, description=""):
    if state.get("cancel_process"):
        raise MySpecialError(f"Process cancelled by user during: {description}")

# GLOBALS FOR THE PLAYWRIGHT WORKER
playwright_job_queue = None
WORKER_EVENT_LOOP = None

async def playwright_worker_main():
    global playwright_job_queue, WORKER_EVENT_LOOP

    WORKER_EVENT_LOOP = asyncio.get_running_loop()
    
    from playwright.async_api import async_playwright
    pw_instance = await async_playwright().start()
    
    print("PROXY DISABLED FOR TESTING - Using direct connection")
    
    browser: Browser = await pw_instance.chromium.launch(
        headless=False,  # Keep visible for debugging
        args=[
            "--no-sandbox", 
            "--disable-dev-shm-usage", 
            "--window-size=1920,1080",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--start-maximized",
            "--force-device-scale-factor=1",
            "--disable-backgrounding-occluded-windows",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection"
        ],
        slow_mo=1000,  # 1 second delay for visibility
        devtools=True,
        channel="chrome"
    )
    print("Playwright worker: browser launched.")

    playwright_job_queue = asyncio.Queue()

    while True:
        job_details = await playwright_job_queue.get()
        if job_details is None:  # Shutdown sentinel
            break

        # Unpack job details
        (sid, domains_list, designations_list, location_list2,
         converted_list, required_counts, num_results_arg, downloadFileName_arg) = job_details
        
        state = clients_state.get(sid)
        if state is None or state.get("cancel_process"):
            playwright_job_queue.task_done()
            continue

        # Spawn a new asyncio.Task for each job
        asyncio.create_task(
            handle_one_job(
                browser, pw_instance, sid, domains_list, designations_list,
                location_list2, converted_list, required_counts,
                num_results_arg, downloadFileName_arg
            )
        )

    # Cleanup
    print("Playwright worker: shutting down browser and Playwright.")
    await browser.close()
    await pw_instance.stop()
    print("Playwright worker: shutdown complete.")

async def handle_one_job(
    browser: Browser, pw_instance, sid: str,
    domains: list, designations: list, locations: list,
    cookies: list, required_counts: dict, num_results_arg: int, downloadFileName: str
):
    state = clients_state.get(sid)
    if state is None or state.get("cancel_process"):
        if playwright_job_queue: 
            playwright_job_queue.task_done()
        return

    context: BrowserContext = None
    page: Page = None
    result_message = "Processed successfully."
    
    # Data accumulation lists
    file_data = []
    file_data2 = []
    emailcount = 0
    temp_file_data2_id = 1
    processed_count = 0
    total_domains = len(domains)

    # Clear previous results
    state["df_list"].clear()
    state["preview_list"].clear()

    try:
        await check_cancel(state, "Job initialization")
        print(f"[{sid}] Creating new browser context for job...")
        
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
        
        # Force browser window to be visible
        try:
            await page.bring_to_front()
            print(f"[{sid}] Browser window brought to front")
        except Exception as e:
            print(f"[{sid}] Could not bring browser to front: {e}")
        
        state["current_context"] = context
        state["current_page"] = page
        print(f"[{sid}] New browser context and page created.")

        # Navigate to Snov.io
        print(f"[{sid}] Navigating to Snov.io...")
        await page.goto("https://app.snov.io/prospects/list", wait_until="networkidle")
        await asyncio.sleep(3)
        await check_cancel(state, "Navigated to SNOV list page")

        # Add cookies if provided
        if cookies:
            print(f"[{sid}] Attempting to add cookies...")
            await context.clear_cookies()
            playwright_cookies_to_add = []
          
            for sel_cookie_orig in cookies:
                cookie = sel_cookie_orig.copy()
                if "sameSite" in cookie and cookie["sameSite"] not in ["Strict", "Lax", "None"]:
                    cookie.pop("sameSite")
                if "expirationDate" in cookie:
                    if not isinstance(cookie["expirationDate"], (int, float)):
                        try:
                            cookie["expirationDate"] = float(cookie["expirationDate"])
                        except ValueError:
                            cookie.pop("expirationDate", None)
                    if isinstance(cookie["expirationDate"], float):
                         cookie["expirationDate"] = int(cookie["expirationDate"])

                pw_cookie = {
                    "name": cookie.get("name"), 
                    "value": cookie.get("value"),
                    "domain": cookie.get("domain"), 
                    "path": cookie.get("path", "/"),
                    "httpOnly": bool(cookie.get("httpOnly", False)),
                    "secure": bool(cookie.get("secure", False))
                }
                if "expirationDate" in cookie and cookie.get("expirationDate") is not None:
                    pw_cookie["expires"] = cookie["expirationDate"]
                if "sameSite" in cookie:
                    pw_cookie["sameSite"] = cookie["sameSite"]
                
                if not (pw_cookie["name"] and pw_cookie["domain"]):
                    continue
                playwright_cookies_to_add.append(pw_cookie)
            
            if playwright_cookies_to_add:
                try:
                    await context.add_cookies(playwright_cookies_to_add)
                    print(f"[{sid}] Added {len(playwright_cookies_to_add)} cookies.")
                except Exception as e:
                    print(f"[{sid}] Warning: Could not add cookies: {e}")
            
            await page.reload(wait_until="networkidle")
            print(f"[{sid}] Page reloaded after adding cookies.")
            await asyncio.sleep(5)

        # Take screenshot for debugging
        screenshot_path = f"debug_before_search_{sid}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"[{sid}] Screenshot saved: {screenshot_path}")
        
        # Check current page status
        current_url = page.url
        page_title = await page.title()
        print(f"[{sid}] Current page: {current_url}")
        print(f"[{sid}] Page title: {page_title}")
        
        # Check if we're on the login page
        if "login" in current_url.lower() or "Log In to your account" in page_title:
            print(f"[{sid}] ERROR: Still on login page. Cookies may be invalid or expired.")
            raise MySpecialError("Authentication failed - stuck on login page. Please provide fresh cookies.")
        
        # Navigate to prospects page
        print(f"[{sid}] Navigating to prospects page...")
        await page.goto("https://app.snov.io/prospects/list", wait_until="networkidle")
        await asyncio.sleep(3)
        
        # Check if redirected back to login
        current_url_after = page.url
        if "login" in current_url_after.lower():
            print(f"[{sid}] ERROR: Redirected back to login page. Authentication failed.")
            raise MySpecialError("Authentication failed - redirected to login. Please provide fresh cookies.")
        
        # Look for the "Create new list" button with improved selectors
        print(f"[{sid}] Looking for list creation button...")
        
        add_list_btn = None
        selectors_to_try = [
            'button[data-test="snov-btn"]:has-text("Create")',
            'button:has-text("Create new list")',
            'button:has-text("Create list")',
            'button:has-text("New list")',
            '.list__toolbar button:first-child',
            'button.snv-btn:has(svg)',
            'button[class*="list"]:has(svg)'
        ]
        
        for selector in selectors_to_try:
            try:
                add_list_btn = await page.wait_for_selector(selector, timeout=5000, state="visible")
                if add_list_btn:
                    print(f"[{sid}] Found list creation button with selector: {selector}")
                    break
            except PlaywrightTimeoutError:
                continue
        
        if not add_list_btn:
            # Take failure screenshot
            failure_screenshot = f"debug_selector_failure_{sid}.png"
            await page.screenshot(path=failure_screenshot, full_page=True)
            print(f"[{sid}] Failure screenshot saved: {failure_screenshot}")
            raise MySpecialError("Could not find list creation button")
        
        # Click the create list button
        await add_list_btn.click()
        await asyncio.sleep(1)

        # Fill in the list name in the modal
        modal_title_loc = page.locator("div.modal-snovio__title", has_text="Create a new prospects list")
        await modal_title_loc.wait_for(state="visible", timeout=10000)
        
        name_input_modal_loc = page.locator("div.modal-snovio__window input.snov-input__input")
        await name_input_modal_loc.wait_for(state="visible", timeout=10000)
        await name_input_modal_loc.fill(str(downloadFileName))
        await asyncio.sleep(0.5)

        # Click create button in modal
        create_btn_modal_loc = page.locator("button[data-test='snov-modal-btn-primary']", has_text="Create")
        await create_btn_modal_loc.click()
        await page.wait_for_selector("div.modal-snovio__window", state="hidden", timeout=10000)
        await check_cancel(state, "Created list")

        # Navigate to database search
        await page.goto("https://app.snov.io/database-search/prospects", wait_until="networkidle")
        await asyncio.sleep(3)
        await check_cancel(state, "Navigated to database search")
       
        # Process each domain
        for domain_idx, domain_str_item in enumerate(domains):
            await check_cancel(state, f"Starting domain {domain_str_item}")
            processed_count += 1
            remaining_calc = total_domains - processed_count
            
            socketio.emit(
                "overall_progress",
                {"total": total_domains, "processed": processed_count, "remaining": remaining_calc},
                room=sid,
            )
            
            print(f"[{sid}] Processing domain: {domain_str_item}")
            socketio.emit(
                "progress_update",
                {"domain": domain_str_item, "message": "Domain processing started"},
                room=sid,
            )

            try:
                # Clear previous company filter
                clear_company_selector = (
                    "div.snov-filter__block:has(div.snov-filter__name:has-text(\"Company name\")) "
                    "span.snov-filter__block-clear"
                )
                try:
                    clear_company_btn = await page.query_selector(clear_company_selector)
                    if clear_company_btn and await clear_company_btn.is_visible():
                        await clear_company_btn.click(timeout=2000)
                        print(f"[{sid}] Cleared 'Company name' filter")
                        await asyncio.sleep(0.5)
                except Exception as e:
                    print(f"[{sid}] Could not clear company filter: {e}")

                # Enter company name
                company_name_input_xpath = "//input[@placeholder='Enter company name']"
                company_name_input_el = await page.wait_for_selector(
                    company_name_input_xpath, state="visible", timeout=10000
                )
                await company_name_input_el.click()
                await company_name_input_el.fill(domain_str_item)
                print(f"[{sid}] Entered '{domain_str_item}' into company name input.")

                # Wait for and click first suggestion
                first_suggestion_xpath = (
                    "//input[@placeholder='Enter company name']"
                    "/ancestor::div[contains(@class, 'snov-filter__block')]"
                    "/following-sibling::ul[contains(@class, 'snov-filter__list')]/div"
                    "/div[contains(@class, 'snov-filter__option')][1]"
                )
                
                first_sugg_el = await page.wait_for_selector(
                    first_suggestion_xpath, state="visible", timeout=10000 
                )
                await first_sugg_el.click()
                print(f"[{sid}] Clicked first company suggestion for '{domain_str_item}'.")
                await asyncio.sleep(1)

                # Process each designation
                for title_str_item in designations:
                    await check_cancel(state, f"Processing designation '{title_str_item}' for domain '{domain_str_item}'")
                    
                    socketio.emit("progress_update", {
                        "domain": domain_str_item, 
                        "message": f"Processing designation: {title_str_item}"
                    }, room=sid)

                    # Clear job title filter
                    clear_job_title_selector = (
                        "div.snov-filter__block:has(div.snov-filter__name:has-text(\"Job title\")) "
                        "span.snov-filter__block-clear"
                    )
                    try:
                        clear_job_btn = await page.query_selector(clear_job_title_selector)
                        if clear_job_btn and await clear_job_btn.is_visible():
                            await clear_job_btn.click(timeout=2000)
                            await asyncio.sleep(0.5)
                    except Exception as e:
                        print(f"[{sid}] Could not clear job title filter: {e}")

                    # Enter job title
                    job_title_input_xpath = (
                        "//div[contains(@class,'snov-filter')][.//span[text()='Job title']]"
                        "//input[contains(@class,'snov-filter__block-input')]"
                    )
                    job_title_input_el = await page.wait_for_selector(
                        job_title_input_xpath, state="visible", timeout=10000
                    )
                    await job_title_input_el.fill(title_str_item)
                    await job_title_input_el.press("Enter")
                    await asyncio.sleep(0.5)

                    # Click search button
                    search_button_xpath = "//button[.//span[text()='Search']]"
                    search_btn = await page.wait_for_selector(
                        search_button_xpath, state="visible", timeout=10000
                    )
                    await search_btn.click()
                    print(f"[{sid}] Search initiated for domain: {domain_str_item}, title: {title_str_item}")
                    await asyncio.sleep(3)

                    # Check for "no prospects" message
                    no_prospects_msg_xpath = "//div[@class='not-found__title' and normalize-space(.)=\"We couldn't find any prospects that match your search\"]"
                    try:
                        await page.wait_for_selector(no_prospects_msg_xpath, state="visible", timeout=5000)
                        print(f"[{sid}] No prospects found for '{title_str_item}' in domain '{domain_str_item}'.")
                        continue
                    except PlaywrightTimeoutError:
                        print(f"[{sid}] Prospects potentially found for '{title_str_item}'.")

                    # Wait for results table
                    try:
                        await page.wait_for_selector("table tbody tr", state="attached", timeout=10000)
                    except PlaywrightTimeoutError:
                        print(f"[{sid}] No table rows found for title '{title_str_item}'.")
                        continue

                    # Process prospects
                    prospect_rows = await page.query_selector_all("table tbody tr")
                    print(f"[{sid}] Found {len(prospect_rows)} prospects to process")
                    
                    required_count = required_counts.get(title_str_item, 5)  # Default to 5 if not specified
                    prospects_added = 0
                    
                    for row_index, row in enumerate(prospect_rows[:required_count], 1):
                        try:
                            # Extract prospect information
                            name_cell = await row.query_selector("td:nth-child(2)")
                            prospect_name = ""
                            if name_cell:
                                name_link = await name_cell.query_selector("a")
                                if name_link:
                                    prospect_name = (await name_link.inner_text()).strip()
                                else:
                                    prospect_name = (await name_cell.inner_text()).strip()
                            
                            print(f"[{sid}] Processing prospect {row_index}: {prospect_name}")
                            
                            # Find and click "Add to list" button
                            add_to_list_btn = await row.query_selector("td:nth-child(5) button[data-test='snov-btn']")
                            if not add_to_list_btn:
                                add_to_list_btn = await row.query_selector("button:has-text('Add to list')")
                            
                            if add_to_list_btn:
                                await add_to_list_btn.click()
                                await asyncio.sleep(2)  # Wait for dropdown
                                
                                # Look for dropdown with list options
                                dropdown_items = await page.query_selector_all("div.app-dropdown__drop-item")
                                if not dropdown_items:
                                    dropdown_items = await page.query_selector_all(".dropdown-item")
                                
                                target_list_found = False
                                for dd_item in dropdown_items:
                                    try:
                                        item_text = (await dd_item.inner_text()).strip()
                                        if downloadFileName.lower() in item_text.lower():
                                            await dd_item.click()
                                            print(f"[{sid}] Added {prospect_name} to list '{item_text}'")
                                            prospects_added += 1
                                            emailcount += 1
                                            target_list_found = True
                                            
                                            socketio.emit("progress_update", {
                                                "domain": domain_str_item,
                                                "message": f"Added {prospect_name} to list ({prospects_added}/{required_count})"
                                            }, room=sid)
                                            
                                            await asyncio.sleep(1)
                                            break
                                    except Exception as e:
                                        print(f"[{sid}] Error with dropdown item: {e}")
                                        continue
                                
                                if not target_list_found:
                                    print(f"[{sid}] Target list '{downloadFileName}' not found for {prospect_name}")
                                    await page.keyboard.press("Escape")  # Close dropdown
                            else:
                                print(f"[{sid}] 'Add to list' button not found for {prospect_name}")
                                
                        except Exception as e_row:
                            print(f"[{sid}] Error processing row {row_index}: {e_row}")
                            continue
                    
                    print(f"[{sid}] Added {prospects_added}/{required_count} prospects for '{title_str_item}'")
                    
                    if prospects_added >= required_count:
                        break  # Move to next domain if we have enough prospects

            except Exception as e_domain:
                print(f"[{sid}] Error processing domain '{domain_str_item}': {e_domain}")
                socketio.emit("progress_update", {
                    "domain": domain_str_item, 
                    "message": "Domain processing failed"
                }, room=sid)
                continue

        # Final success message
        socketio.emit("process_data_response", {
            "message": f"Processing completed! Added {emailcount} prospects to list '{downloadFileName}'"
        }, room=sid)

    except MySpecialError as e:
        result_message = f"Process cancelled or failed: {str(e)}"
        print(f"[{sid}] {result_message}")
        socketio.emit("process_data_response", {
            "message": result_message,
            "error": "ProcessCancelled"
        }, room=sid)
    except Exception as e:
        result_message = f"Unexpected error: {str(e)}"
        print(f"[{sid}] {result_message}")
        socketio.emit("process_data_response", {
            "message": result_message,
            "error": "UnexpectedError"
        }, room=sid)
    finally:
        # Cleanup
        if context:
            try:
                await context.close()
                print(f"[{sid}] Browser context closed.")
            except Exception as e:
                print(f"[{sid}] Error closing context: {e}")
        
        state["current_context"] = None
        state["current_page"] = None
        
        print(f"[{sid}] Job handle_one_job finished. Result message: {result_message}")
        if playwright_job_queue: 
            playwright_job_queue.task_done()

# Start the worker thread
def start_playwright_worker_thread():
    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(playwright_worker_main())
        except KeyboardInterrupt:
            print("Worker thread interrupted. Shutting down...")
        finally:
            loop.close()
            print("Worker thread event loop closed.")

    thread = threading.Thread(target=run_loop, daemon=True, name="PlaywrightWorkerThread")
    thread.start()
    print("Playwright worker thread started.")
    return thread

worker_thread = start_playwright_worker_thread()

# SOCKETIO EVENT HANDLERS
@app.route("/")
def index():
    return "Flask-SocketIO + Async Playwright Worker is UP."

@socketio.on("connect")
def on_connect():
    sid = request.sid
    clients_state[sid] = {
        "df_list": [],
        "preview_list": [],
        "cancel_process": False,
        "current_context": None,
        "current_page": None,
    }
    print(f"Client connected: {sid}")

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    print(f"Client disconnected: {sid}")
    state = clients_state.get(sid)
    if state:
        state["cancel_process"] = True
        if state.get("current_context"):
            print(f"[{sid}] Disconnect: current_context exists. Cancellation flag set.")
    clients_state.pop(sid, None)

@socketio.on("process_data")
def handle_process_data(payload):
    sid = request.sid
    state = clients_state.get(sid)
    if not state:
        emit("process_data_response", {
            "message": "Error: Client state not found. Please reconnect.", 
            "error": "StateNotFound"
        }, room=sid)
        return

    # Reset cancellation & clear previous results
    state["cancel_process"] = False
    state["df_list"].clear()
    state["preview_list"].clear()
    state["current_context"] = None
    state["current_page"] = None

    try:
        domains_str = payload.get('domains', '')
        domains_list = [get_domain(d) for d in domains_str.splitlines() if d.strip() and get_domain(d)]

        designations_payload = payload.get('designations', '')
        downloadFileName_from_payload = payload.get('downloadFileName', 'Scraped_Prospects_List') 
        
        designations_list_raw = [d.strip() for d in designations_payload.split(',') if d.strip()]
        
        location_payload = payload.get('location', '')
        location_list_parsed = [l.strip().lower() for l in location_payload.split(',') if l.strip()]
        
        num_results_arg = int(payload.get('numResults', 10))
        
        # Parse array data (cookies)
        array_data_str = payload.get('arrayData', '[]')
        try:
            converted_list = ast.literal_eval(array_data_str) if array_data_str else []
        except (ValueError, SyntaxError):
            converted_list = []

        # Create required counts dictionary
        required_counts = {}
        for designation in designations_list_raw:
            required_counts[designation] = min(5, num_results_arg)  # Max 5 per designation

        print(f"[{sid}] Processing {len(domains_list)} domains with {len(designations_list_raw)} designations")

        # Submit job to worker queue
        if WORKER_EVENT_LOOP and playwright_job_queue:
            job_details = (
                sid, domains_list, designations_list_raw, location_list_parsed,
                converted_list, required_counts, num_results_arg, downloadFileName_from_payload
            )
            
            # Schedule the job on the worker thread
            asyncio.run_coroutine_threadsafe(
                playwright_job_queue.put(job_details), 
                WORKER_EVENT_LOOP
            )
            
            emit("process_data_response", {
                "message": "Job submitted successfully. Processing started."
            }, room=sid)
        else:
            emit("process_data_response", {
                "message": "Error: Worker not ready. Please try again.",
                "error": "WorkerNotReady"
            }, room=sid)

    except Exception as e:
        print(f"[{sid}] Error in handle_process_data: {e}")
        emit("process_data_response", {
            "message": f"Error processing request: {str(e)}",
            "error": "ProcessingError"
        }, room=sid)

@socketio.on("refresh")
def handle_refresh():
    sid = request.sid
    state = clients_state.get(sid)
    if not state:
        return

    print(f"[{sid}] Refresh request received.")
    state["cancel_process"] = True

    if state.get("current_context"):
         print(f"[{sid}] Refresh: current_context exists. Cancellation flag set.")
    
    state["current_context"] = None
    state["current_page"] = None
    state["df_list"].clear()
    state["preview_list"].clear()

    emit("refresh_response", {
        "message": "Application state refreshed successfully. Ongoing process will be cancelled."
    }, room=sid)

# RUN THE SERVER
if __name__ == "__main__":
    import time
    time.sleep(2)  # Wait for worker thread to be ready
    
    socketio.run(
        app,
        host="127.0.0.1",
        port=5000,
        debug=True
    )

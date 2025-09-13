
import asyncio
import threading
import ast
import re
import pandas as pd
import time # For general time utilities, not for blocking sleeps in async code
import random
import string
import io
import base64
from urllib.parse import urlparse
import heapq
from flask import Flask, request
from flask_cors import CORS
from proxy_config import proxy_rotator
from flask_socketio import SocketIO, emit

# --- Playwright ASYNC Imports ---
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
    Browser, # For type hinting
    BrowserContext,
    Page,
    ElementHandle
)

# 
# > IMPORTANT: We use the ASYNC Playwright API inside a dedicated worker thread.
#   That thread will host its own asyncio event loop. All Playwright calls
#   (browser.new_context, page.goto, etc.) run as asyncio coroutines on that loop.
#
#   Because every Playwright coroutine runs in the same OS thread (the worker),
#   there is never a “greenlet switching threads” mismatch. And yet multiple
#   contexts can be created concurrently, each in its own asyncio.Task.
#

# 1) FLASK + SOCKETIO SETUP 
app = Flask(__name__)
CORS(app)

seven_days = 7 * 24 * 60 * 60
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins="*",
    allow_upgrades=True,
    logger=True,
    engineio_logger=True
)

print("SocketIO async_mode =", socketio.async_mode) # Should print "threading"

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
    domain = parsed.netloc.replace('www.', '').strip()
    return domain

async def check_cancel(state, description=""): # Made async if it needs to await asyncio.sleep(0)
    if state.get("cancel_process"):
        # Optionally, yield control to allow event loop to process other tasks
        # await asyncio.sleep(0) # Not strictly necessary here, but good for long checks
        raise MySpecialError(f"Process cancelled by user during: {description}")

# ───────────── 2) GLOBALS FOR THE PLAYWRIGHT WORKER ──────────────────────────
playwright_job_queue = None     # Will be set to an asyncio.Queue() inside worker
WORKER_EVENT_LOOP = None        # To store the worker's event loop instance

async def playwright_worker_main():
    global playwright_job_queue, WORKER_EVENT_LOOP

    WORKER_EVENT_LOOP = asyncio.get_running_loop() 
    """ 
    asyncio.get_running_loop() returns the asyncio event loop object that’s actively running in the current OS thread.
    By assigning it to WORKER_EVENT_LOOP, you now have a handle to that loop which you can use later to schedule new tasks,
    create timers, run callbacks, or shut the loop down.
    This is useful if you need to call loop methods from places that don’t naturally have loop access
    """
    from playwright.async_api import async_playwright # Import here to be in the right context
    pw_instance = await async_playwright().start()
    
    # Temporarily disable proxy for testing - uncomment next lines to re-enable
    # proxy_config = proxy_rotator.get_next_proxy()
    # print(f"→ Using proxy: {proxy_config.get('server') if proxy_config else 'No proxy'}")
    
    # # Test proxy if available
    # if proxy_config:
    #     proxy_working = await proxy_rotator.test_proxy(proxy_config)
    #     if not proxy_working:
    #         print(f"→ Proxy test failed, marking as failed and trying next...")
    #         proxy_rotator.mark_proxy_failed(proxy_config)
    #         proxy_config = proxy_rotator.get_next_proxy()
    
    proxy_config = None  # Disable proxy for testing
    print(f"PROXY DISABLED FOR TESTING - Using direct connection")
    
    browser: Browser = await pw_instance.chromium.launch(
        headless=False, # Set to True for production if no UI is needed
        # proxy=proxy_config,  # Temporarily disabled
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
            "--disable-ipc-flooding-protection",
            "--new-window",
            "--disable-extensions-except",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-popup-blocking",
            "--disable-translate",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--force-color-profile=srgb",
            "--disable-background-mode",
            "--disable-hang-monitor",
            "--disable-prompt-on-repost",
            "--disable-sync",
            "--metrics-recording-only",
            "--no-experiments",
            "--use-mock-keychain",
            "--force-fieldtrials=*BackgroundTracing/default/",
            "--aggressive-cache-discard",
            "--memory-pressure-off"
        ],
        slow_mo=2000,  # Increased to 2 seconds for even better visibility
        devtools=True,  # Open DevTools - this forces the browser to be visible
        channel="chrome"  # Use system Chrome instead of Chromium
    )
    print("Playwright worker: browser launched.")

    playwright_job_queue = asyncio.Queue()

    while True:
        job_details = await playwright_job_queue.get()
        if job_details is None:  # Shutdown sentinel
            break

        # Unpack job details
        ( sid, domains_list, designations_list, location_list2,
          converted_list, required_counts, num_results_arg, downloadFileName_arg ) = job_details
        state = clients_state.get(sid)
        if state is None or state.get("cancel_process"): # Check cancel flag before starting
            playwright_job_queue.task_done()
            continue

        # Process job directly instead of creating a task
        await handle_one_job(
            browser, pw_instance, sid, domains_list, designations_list,
            location_list2, converted_list, required_counts,
            num_results_arg, downloadFileName_arg
        )
        playwright_job_queue.task_done()

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
        
        # Temporarily disable job-level proxy for testing
        # job_proxy_config = proxy_rotator.get_random_proxy()
        # if job_proxy_config:
        #     print(f"[{sid}] Using proxy for this job: {job_proxy_config.get('server')}")
        #     context = await browser.new_context(
        #         no_viewport=True,
        #         proxy=job_proxy_config
        #     )
        # else:
        #     print(f"[{sid}] No proxy available, using direct connection")
        
        job_proxy_config = None  # Disable for testing
        print(f"[{sid}] PROXY DISABLED - Using direct connection for this job")
        context = await browser.new_context(no_viewport=True)
        
        # Force browser window to be visible and in foreground
        print(f"[{sid}] Opening new page and bringing to foreground...")
        page = await context.new_page()
        
        # Try to bring the browser window to the front
        try:
            await page.bring_to_front()
            print(f"[{sid}] Browser window brought to front")
        except Exception as e:
            print(f"[{sid}] Could not bring browser to front: {e}")
        
        # Navigate to a test page first to ensure browser is visible
        await page.goto("https://www.google.com")
        await asyncio.sleep(5)  # Increased wait time
        print(f"[{sid}] Browser window should now be visible at Google homepage")
        
        # Try Windows-specific methods to bring browser to foreground
        try:
            import ctypes
            from ctypes import wintypes
            
            # Get the browser window handle
            def enum_windows_proc(hwnd, lParam):
                if ctypes.windll.user32.IsWindowVisible(hwnd):
                    window_text = ctypes.create_unicode_buffer(512)
                    ctypes.windll.user32.GetWindowTextW(hwnd, window_text, 512)
                    if "Google" in window_text.value or "Chrome" in window_text.value or "Chromium" in window_text.value:
                        # Force window to foreground
                        ctypes.windll.user32.SetForegroundWindow(hwnd)
                        ctypes.windll.user32.ShowWindow(hwnd, 9)  # SW_RESTORE
                        ctypes.windll.user32.SetWindowPos(hwnd, -1, 0, 0, 0, 0, 0x0001 | 0x0002)  # HWND_TOPMOST
                        print(f"[{sid}] Forced browser window to foreground using Windows API")
                        return False  # Stop enumeration
                return True
            
            # Enumerate all windows and find browser
            EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
            ctypes.windll.user32.EnumWindows(EnumWindowsProc(enum_windows_proc), 0)
            
        except Exception as e_windows:
            print(f"[{sid}] Windows API approach failed: {e_windows}")
        
        # Wait longer for user to see the browser window
        print(f"[{sid}] Browser window is now visible. Waiting 15 seconds before proceeding...")
        print(f"[{sid}] You should see Google homepage in the browser window.")
        print(f"[{sid}] If you don't see it, check your taskbar for the Chrome window.")
        await asyncio.sleep(15)  # Longer wait time to ensure visibility
        print(f"[{sid}] Proceeding with Snov.io automation...")
        
        state["current_context"] = context # For potential cancellation by refresh/disconnect
        state["current_page"] = page
        print(f"[{sid}] New browser context and page created.")

        # Now navigate to Snov.io
        print(f"[{sid}] Navigating to Snov.io...")
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

        # Take screenshot before attempting to find the button
        screenshot_path = f"debug_before_search_{sid}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"[{sid}] Screenshot saved: {screenshot_path}")
        
        # Log current page URL and title for debugging
        current_url = page.url
        page_title = await page.title()
        print(f"[{sid}] Current page: {current_url}")
        print(f"[{sid}] Page title: {page_title}")
        
        # Check if we're on the login page instead of dashboard
        if "login" in current_url.lower() or "Log In to your account" in page_title:
            print(f"[{sid}] WARNING: On login page. Cookies may be invalid or expired.")
            print(f"[{sid}] Current URL: {current_url}")
            print(f"[{sid}] Page title: {page_title}")
            print(f"[{sid}] Continuing without authentication - manual login may be required.")
            # Don't raise error immediately, let user manually login
            socketio.emit("progress_update", {
                "domain": "Authentication", 
                "message": "Please manually login to Snov.io in the browser window"
            }, room=sid)
            await asyncio.sleep(30)  # Give user time to login manually
        
        # Check if we're on the right page
        if "snov.io" not in current_url:
            print(f"[{sid}] WARNING: Not on Snov.io page. Current URL: {current_url}")
        
        # Navigate directly to prospects page since we're logged in
        print(f"[{sid}] Navigating directly to prospects page...")
        await page.goto("https://app.snov.io/prospects/list")
        await asyncio.sleep(3)
        
        # Check if we're still on login page after navigation
        current_url_after = page.url
        page_title_after = await page.title()
        print(f"[{sid}] After navigation - URL: {current_url_after}")
        print(f"[{sid}] After navigation - Title: {page_title_after}")
        
        if "login" in current_url_after.lower() or "Log In to your account" in page_title_after:
            print(f"[{sid}] ERROR: Redirected back to login page. Authentication definitely failed.")
            raise MySpecialError("Authentication failed - redirected to login. Please provide fresh cookies.")
        
        add_list_icon_btn_el = None
        
        # Use the exact selector from the provided HTML structure
        print(f"[{sid}] Looking for list creation button...")
        
        try:
            # Most specific selector based on the HTML structure
            add_list_icon_btn_el = await page.wait_for_selector(
                'div.list__toolbar button[data-test="snov-btn"]:has(use[xlink\\:href="#list_add_icon"])', 
                timeout=10_000, 
                state="visible"
            )
            print(f"[{sid}] Found list creation button with specific selector!")
        except PlaywrightTimeoutError:
            print(f"[{sid}] Specific selector failed, trying broader approach...")
            
            try:
                # Find the use element with list_add_icon and get its parent button
                use_element = await page.wait_for_selector('use[xlink\\:href="#list_add_icon"]', timeout=5_000, state="visible")
                add_list_icon_btn_el = await use_element.query_selector("xpath=ancestor::button[1]")
                print(f"[{sid}] Found button via use element!")
            except PlaywrightTimeoutError:
                print(f"[{sid}] Could not find list_add_icon use element")
                
                # Take screenshot for debugging
                failure_screenshot = f"debug_selector_failure_{sid}.png"
                await page.screenshot(path=failure_screenshot, full_page=True)
                print(f"[{sid}] Failure screenshot saved: {failure_screenshot}")
                
                # Log available use elements - check all of them to find list_add_icon
                all_use_elements = await page.query_selector_all('use')
                print(f"[{sid}] Found {len(all_use_elements)} use elements on page")
                
                list_add_icon_found = False
                for i, use_el in enumerate(all_use_elements):
                    try:
                        href = await use_el.get_attribute("xlink:href")
                        if "#list_add_icon" in str(href):
                            print(f"[{sid}] FOUND list_add_icon at position {i+1}: href='{href}'")
                            list_add_icon_found = True
                            # Try to get the button ancestor
                            try:
                                add_list_icon_btn_el = await use_el.query_selector("xpath=ancestor::button[1]")
                                if add_list_icon_btn_el:
                                    print(f"[{sid}] Successfully found button ancestor for list_add_icon!")
                                    break
                            except Exception as e_ancestor:
                                print(f"[{sid}] Error finding button ancestor: {e_ancestor}")
                        elif i < 20:  # Only log first 20 for brevity
                            print(f"[{sid}] Use element {i+1}: href='{href}'")
                    except:
                        if i < 20:
                            print(f"[{sid}] Use element {i+1}: could not read href")
                
                if not list_add_icon_found:
                    print(f"[{sid}] ERROR: #list_add_icon not found in any of the {len(all_use_elements)} use elements")
                    print(f"[{sid}] This suggests the page structure may have changed or the element is not loaded yet")
                    
                    # Try alternative approaches to find the add list button
                    print(f"[{sid}] Trying alternative approaches...")
                    
                    # Look for buttons with specific classes or text that might be the add list button
                    alternative_selectors = [
                        'button.list__btn:first-child',  # First button in list toolbar
                        'div.list__toolbar button:first-child',  # First button in toolbar
                        'button[class*="list"]:has(svg)',  # Button with "list" in class that has SVG
                        'button.snv-btn:has(svg):first-of-type'  # First snv-btn with SVG
                    ]
                    
                    for selector in alternative_selectors:
                        try:
                            alt_button = await page.query_selector(selector)
                            if alt_button and await alt_button.is_visible():
                                print(f"[{sid}] Found alternative button with selector: {selector}")
                                add_list_icon_btn_el = alt_button
                                break
                        except Exception as e_alt:
                            print(f"[{sid}] Alternative selector '{selector}' failed: {e_alt}")
                
                if not add_list_icon_btn_el:
                    raise MySpecialError("Could not find list creation button")
        
        if not add_list_icon_btn_el: 
            # Final screenshot before giving up
            final_screenshot = f"debug_final_failure_{sid}.png"
            await page.screenshot(path=final_screenshot, full_page=True)
            print(f"[{sid}] Final failure screenshot saved: {final_screenshot}")
            raise MySpecialError("Could not find 'add list' icon button.")
        
        # Click the add list button with better error handling
        print(f"[{sid}] Clicking add list button...")
        try: 
            await add_list_icon_btn_el.click(timeout=5000)
        except PlaywrightTimeoutError: 
            print(f"[{sid}] Regular click failed, trying JavaScript click...")
            await page.evaluate("el => el.click()", add_list_icon_btn_el)
        
        await asyncio.sleep(2)  # Increased wait time for modal to appear
        print(f"[{sid}] Waiting for modal to appear...")

        # Take screenshot to debug modal state
        modal_debug_screenshot = f"debug_modal_state_{sid}.png"
        await page.screenshot(path=modal_debug_screenshot, full_page=True)
        print(f"[{sid}] Modal debug screenshot saved: {modal_debug_screenshot}")

        # Try multiple modal selectors
        modal_appeared = False
        modal_selectors = [
            "div.modal-snovio__title:has-text('Create a new prospects list')",
            "div.modal-snovio__window",
            ".modal-title:has-text('Create')",
            ".modal:has-text('Create')",
            "[role='dialog']"
        ]
        
        for selector in modal_selectors:
            try:
                await page.wait_for_selector(selector, state="visible", timeout=5000)
                print(f"[{sid}] Modal found with selector: {selector}")
                modal_appeared = True
                break
            except PlaywrightTimeoutError:
                continue
        
        if not modal_appeared:
            print(f"[{sid}] Modal did not appear. Checking page content...")
            page_content = await page.content()
            if "Create" in page_content and "list" in page_content.lower():
                print(f"[{sid}] Modal content exists but not detected by selectors")
            else:
                print(f"[{sid}] No modal content found on page")
            raise MySpecialError("List creation modal did not appear after clicking button")
        
        # Find and fill the input field with multiple approaches
        input_filled = False
        input_selectors = [
            "div.modal-snovio__window input.snov-input__input",
            "input[placeholder*='name']",
            ".modal input[type='text']",
            "input.form-control",
            ".modal-body input"
        ]
        
        for input_selector in input_selectors:
            try:
                name_input = await page.wait_for_selector(input_selector, state="visible", timeout=5000)
                await name_input.fill(str(downloadFileName))
                print(f"[{sid}] Filled input with selector: {input_selector}")
                input_filled = True
                break
            except PlaywrightTimeoutError:
                continue
        
        if not input_filled:
            raise MySpecialError("Could not find or fill the list name input field")
        
        await asyncio.sleep(1)

        # Find and click the create button with multiple approaches
        create_clicked = False
        create_selectors = [
            "button[data-test='snov-modal-btn-primary']:has-text('Create')",
            "button:has-text('Create')",
            ".modal-footer button:has-text('Create')",
            ".btn-primary:has-text('Create')",
            "button.btn:has-text('Create')"
        ]
        
        for create_selector in create_selectors:
            try:
                create_btn = await page.wait_for_selector(create_selector, state="visible", timeout=5000)
                await create_btn.scroll_into_view_if_needed()
                
                # Try regular click first, then JavaScript click
                try:
                    await create_btn.click(timeout=3000)
                except PlaywrightTimeoutError:
                    await page.evaluate("el => el.click()", create_btn)
                
                print(f"[{sid}] Clicked create button with selector: {create_selector}")
                create_clicked = True
                break
            except PlaywrightTimeoutError:
                continue
        
        if not create_clicked:
            raise MySpecialError("Could not find or click the Create button in modal")
        
        # Wait for modal to close
        try:
            await page.wait_for_selector("div.modal-snovio__window", state="hidden", timeout=10_000)
            print(f"[{sid}] Modal closed successfully - list created!")
        except PlaywrightTimeoutError:
            # Check if we're on a different page or if list was created
            current_url = page.url
            print(f"[{sid}] Modal didn't close as expected. Current URL: {current_url}")
            # Continue anyway as list might have been created
        
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
                
                # Check if it's a proxy-related error and rotate proxy
                if "net::ERR_PROXY" in str(e_domain_setup) or "timeout" in str(e_domain_setup).lower():
                    print(f"[{sid}] Proxy error detected, rotating to new proxy...")
                    if job_proxy_config:
                        proxy_rotator.mark_proxy_failed(job_proxy_config)
                    
                    # Get new proxy and create new context
                    try:
                        new_proxy_config = proxy_rotator.get_next_proxy()
                        if new_proxy_config:
                            print(f"[{sid}] Switching to new proxy: {new_proxy_config.get('server')}")
                            await context.close()
                            context = await browser.new_context(
                                no_viewport=True,
                                proxy=new_proxy_config
                            )
                            page = await context.new_page()
                            job_proxy_config = new_proxy_config
                            state["current_context"] = context
                            state["current_page"] = page
                            
                            # Retry navigation with new proxy
                            await page.goto("https://app.snov.io/prospects/list")
                            await asyncio.sleep(4)
                            continue  # Skip to next iteration to retry this domain
                        else:
                            print(f"[{sid}] No more proxies available, continuing with direct connection")
                    except Exception as e_proxy_switch:
                        print(f"[{sid}] Failed to switch proxy: {e_proxy_switch}")
                
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
                continue 
         
    
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



                    continue 
                except MySpecialError: raise
                except PlaywrightTimeoutError: # This is the "good" path - no "not found" message means prospects might exist
                    print(f"[{sid}] Prospects potentially found for '{title_str_item}' (no 'not found' message).")
                except Exception as e_no_prospects_check:
                    print(f"[{sid}] Error checking for 'no prospects' message: {e_no_prospects_check}")
                    continue

                try:
                    # Wait for at least one table row to ensure results are loaded
                    # Try specific table first, then fallback to generic table
                    try:
                        await page.wait_for_selector("table[data-v-4f3d4661] tbody tr", state="attached", timeout=10_000)
                    except PlaywrightTimeoutError:
                        await page.wait_for_selector("table.table tbody tr", state="attached", timeout=5_000)
                except MySpecialError: raise
                except PlaywrightTimeoutError:
                    print(f"[{sid}] Timed out waiting for table rows for title '{title_str_item}'.")
                    continue # To next designation
                
              
                async def _async_wait_all_email_cells_final(pg_handle: Page, current_state, current_domain, current_title, timeout_seconds=20.0):
                    start_time_sec = time.monotonic()
                    print(f"[{sid}] Waiting for all email cells in table to stabilize for {current_domain}/{current_title}...")
                    while time.monotonic() - start_time_sec < timeout_seconds:
                        await check_cancel(current_state, f"waiting email states for {current_domain}/{current_title}")
                        # Try specific table first, then fallback to generic selectors
                        all_rows_in_table = await pg_handle.query_selector_all("table[data-v-4f3d4661] tbody tr")
                        if not all_rows_in_table:
                            all_rows_in_table = await pg_handle.query_selector_all("table.table tbody tr")
                        if not all_rows_in_table:
                            all_rows_in_table = await pg_handle.query_selector_all("tbody tr")
                        if not all_rows_in_table:
                            await asyncio.sleep(0.5); continue
                        
                        all_cells_are_final = True
                        for r_idx, r_el_h in enumerate(all_rows_in_table):
                            try:
                                # Email is in the 4th column (index 3) based on the table structure
                                email_cell_el_h = await r_el_h.query_selector("css=td:nth-child(4)")
                                if not email_cell_el_h:
                                    all_cells_are_final = False; break
                                text_in_cell = (await email_cell_el_h.inner_text()).strip()
                                is_final_state = ("@" in text_in_cell or 
                                                  "No email found" in text_in_cell or
                                                  ("Click" in text_in_cell and "Add to list" in text_in_cell) or # Less reliable
                                                  (text_in_cell == "" and await r_el_h.query_selector("css=td:nth-child(5) button span:has-text('Add to list')"))) # Less reliable

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
                    continue 
                
                print(f"[{sid}] --- Processing prospects individually for (Title: '{title_str_item}') ---")
                
                # Initialize heap_processing_rows variable to avoid scope issues
                heap_processing_rows = []
                
                try:
                    # Add detailed logging for table detection
                    print(f"[{sid}] Attempting to find prospect table...")
                    
                    # Try multiple table selectors with detailed logging
                    prospect_rows = await page.query_selector_all("table[data-v-4f3d4661] tbody tr")
                    print(f"[{sid}] Primary selector found {len(prospect_rows)} rows")
                    
                    if not prospect_rows:
                        prospect_rows = await page.query_selector_all("table.table tbody tr")
                        print(f"[{sid}] Fallback selector 1 found {len(prospect_rows)} rows")
                    
                    if not prospect_rows:
                        prospect_rows = await page.query_selector_all("tbody tr")
                        print(f"[{sid}] Fallback selector 2 found {len(prospect_rows)} rows")
                    
                    if not prospect_rows:
                        prospect_rows = await page.query_selector_all("tr.row")
                        print(f"[{sid}] Fallback selector 3 found {len(prospect_rows)} rows")
                    
                    # Track total prospects found for limited prospect logic
                    total_prospects_found = len(prospect_rows)
                    print(f"[{sid}] Total prospects found: {total_prospects_found}")
                    
                    # Define threshold for "limited prospects" - adjust as needed
                    LIMITED_PROSPECTS_THRESHOLD = 20  # If less than 20 prospects found, allow re-adding
                    is_limited_prospects = total_prospects_found < LIMITED_PROSPECTS_THRESHOLD
                    
                    if is_limited_prospects:
                        print(f"[{sid}] LIMITED PROSPECTS DETECTED ({total_prospects_found} < {LIMITED_PROSPECTS_THRESHOLD}) - Will allow re-adding already saved prospects")
                    
                    if not prospect_rows:
                        # Check if table exists at all
                        table_exists = await page.query_selector("table")
                        if table_exists:
                            print(f"[{sid}] Table found but no rows detected. Checking page content...")
                            page_content = await page.content()
                            if "cipla" in page_content.lower() or "director" in page_content.lower():
                                print(f"[{sid}] Page contains prospect data but table structure may have changed")
                            else:
                                print(f"[{sid}] Page may not have loaded prospect data yet")
                        else:
                            print(f"[{sid}] No table found on page")
                        continue
                    
                    print(f"[{sid}] Successfully found {len(prospect_rows)} prospects to process")
                    prospects_added_count = 0
                    
                    # Get required count for this designation
                    required_count_for_designation = required_counts.get(title_str_item, 0)
                    print(f"[{sid}] Required count for '{title_str_item}': {required_count_for_designation}")
                    
                    if required_count_for_designation <= 0:
                        print(f"[{sid}] No prospects required for '{title_str_item}', skipping...")
                        continue
                    
                    # Process each prospect row individually up to required count
                    for row_index, row in enumerate(prospect_rows, 1):
                        # Stop if we've reached the required count
                        if prospects_added_count >= required_count_for_designation:
                            print(f"[{sid}] Reached required count ({required_count_for_designation}) for '{title_str_item}', stopping...")
                            break
                            
                        try:
                            print(f"[{sid}] Processing prospect {row_index}/{len(prospect_rows)} (Target: {required_count_for_designation})")
                            
                            # Extract prospect information with detailed logging
                            name_cell = await row.query_selector("td:nth-child(2)")
                            company_cell = await row.query_selector("td:nth-child(3)")
                            email_cell = await row.query_selector("td:nth-child(4)")
                            
                            prospect_name = ""
                            prospect_company = ""
                            email_status = ""
                            
                            # Debug cell detection
                            print(f"[{sid}] Row {row_index}: name_cell={name_cell is not None}, company_cell={company_cell is not None}, email_cell={email_cell is not None}")
                            
                            if name_cell:
                                name_link = await name_cell.query_selector("a")
                                if name_link:
                                    prospect_name = (await name_link.inner_text()).strip()
                                else:
                                    # Try getting text directly from cell
                                    prospect_name = (await name_cell.inner_text()).strip()
                            
                            if company_cell:
                                company_link = await company_cell.query_selector(".row__cell--company-name a")
                                if company_link:
                                    prospect_company = (await company_link.inner_text()).strip()
                                else:
                                    # Try alternative selectors
                                    alt_company_link = await company_cell.query_selector("a")
                                    if alt_company_link:
                                        prospect_company = (await alt_company_link.inner_text()).strip()
                                    else:
                                        prospect_company = (await company_cell.inner_text()).strip()
                            
                            if email_cell:
                                email_text = (await email_cell.inner_text()).strip()
                                email_status = email_text
                            
                            print(f"[{sid}] Prospect: '{prospect_name}' | Company: '{prospect_company}' | Email: '{email_status}'")
                            
                            # Check if prospect meets preferences - support multiple designations
                            should_add_prospect = False  # Default: don't add unless matches criteria
                            
                            # Define target designations (you can modify this list)
                            target_designations = [
                                "director", "manager", "head", "president", "vp", "vice president",
                                "ceo", "cfo", "cto", "engineer", "lead", "senior", "associate"
                            ]
                            
                            # Check if prospect name contains any target designation
                            prospect_name_lower = prospect_name.lower()
                            for designation in target_designations:
                                if designation in prospect_name_lower:
                                    should_add_prospect = True
                                    print(f"[{sid}] {prospect_name} matches designation filter: '{designation}'")
                                    break
                            
                            # If no designation match, still add if no specific filtering is needed
                            if not should_add_prospect:
                                should_add_prospect = True  # Change to False if you want strict filtering
                                print(f"[{sid}] {prospect_name} - no designation match but adding anyway (general filter)")
                            
                            if should_add_prospect:
                                # Check if prospect is already saved in any list
                                action_cell = await row.query_selector("td:nth-child(5)")
                                is_already_saved = False
                                
                                if action_cell:
                                    # Check for "Saved" button or similar indicators
                                    saved_indicators = [
                                        "button:has-text('Saved')",
                                        "button[class*='saved']",
                                        ".btn-success",
                                        "button[disabled]",
                                        ".prospect-saved",
                                        "span:has-text('Saved')",
                                        "span:has-text('Added')"
                                    ]
                                    
                                    for indicator in saved_indicators:
                                        saved_element = await action_cell.query_selector(indicator)
                                        if saved_element:
                                            saved_text = await saved_element.inner_text()
                                            if any(keyword in saved_text.lower() for keyword in ['saved', 'added', 'in list']):
                                                is_already_saved = True
                                                print(f"[{sid}] {prospect_name} is already saved in a list - skipping")
                                                break
                                    
                                    # Also check if the button text indicates it's already saved
                                    if not is_already_saved:
                                        button_text = await action_cell.inner_text()
                                        if any(keyword in button_text.lower() for keyword in ['saved', 'added', 'in list']):
                                            is_already_saved = True
                                            print(f"[{sid}] {prospect_name} is already saved (button text: '{button_text}') - skipping")
                                
                                if is_already_saved and not is_limited_prospects:
                                    print(f"[{sid}] Skipping {prospect_name} - already saved in another list")
                                    continue
                                elif is_already_saved and is_limited_prospects:
                                    print(f"[{sid}] {prospect_name} is already saved, but re-adding due to limited prospects ({total_prospects_found} total)")
                                
                                # Find the "Add to list" button for this row
                                add_to_list_btn = await row.query_selector("td:nth-child(5) button[data-test='snov-btn']")
                                if not add_to_list_btn:
                                    add_to_list_btn = await row.query_selector(".row__cell--action button")
                                if not add_to_list_btn:
                                    add_to_list_btn = await row.query_selector("button:has-text('Add to list')")
                                
                                if add_to_list_btn:
                                    await add_to_list_btn.click()
                                    print(f"[{sid}] Clicked 'Add to list' for {prospect_name}")
                                    
                                    # Wait for dropdown to appear with enhanced detection
                                    dropdown_items = []
                                    max_attempts = 8
                                    for attempt in range(max_attempts):
                                        await asyncio.sleep(2)  # Increased wait time
                                        print(f"[{sid}] Attempt {attempt + 1}/{max_attempts} to find dropdown...")
                                        
                                        # Enhanced dropdown selectors with more specific ones first
                                        dropdown_selectors = [
                                            "div.app-dropdown__drop-item[data-test='save-to-list']",
                                            "div.snov-dropdown-item",
                                            "div.app-dropdown__drop-item",
                                            ".dropdown-menu .dropdown-item",
                                            ".v-popper--theme-dropdown .pl-select__drop-item",
                                            ".snov-dropdown__options .snov-dropdown-item",
                                            "[data-test='save-to-list']",
                                            ".dropdown-item",
                                            ".list-item",
                                            "[role='option']",
                                            ".menu-item",
                                            "ul li",
                                            ".list-group-item",
                                            ".popper .dropdown-item",
                                            ".v-popper .dropdown-item"
                                        ]
                                        
                                        for selector in dropdown_selectors:
                                            dropdown_items = await page.query_selector_all(f"css={selector}")
                                            if dropdown_items:
                                                print(f"[{sid}] Found {len(dropdown_items)} dropdown items with selector: {selector}")
                                                break
                                        
                                        if dropdown_items:
                                            break
                                        else:
                                            print(f"[{sid}] No dropdown items found on attempt {attempt + 1}")
                                    
                                    # Take screenshot of dropdown state for debugging
                                    dropdown_debug_screenshot = f"debug_dropdown_state_{sid}.png"
                                    await page.screenshot(path=dropdown_debug_screenshot, full_page=True)
                                    print(f"[{sid}] Dropdown debug screenshot saved: {dropdown_debug_screenshot}")
                                    
                                    # If still no dropdown, try to look for any visible elements that might be the dropdown
                                    if not dropdown_items:
                                        print(f"[{sid}] Searching for any visible dropdown-like elements...")
                                        # Look for any recently appeared elements
                                        all_visible = await page.query_selector_all("css=div:visible, li:visible, [role='option']:visible")
                                        print(f"[{sid}] Found {len(all_visible)} visible elements to check")
                                        
                                        # Check for elements that might contain list names
                                        for element in all_visible[-30:]:  # Check last 30 elements (recently appeared)
                                            try:
                                                text = await element.inner_text()
                                                if text and text.strip() and (
                                                    downloadFileName.lower() in text.lower() or 
                                                    "list" in text.lower() or 
                                                    "create" in text.lower() or
                                                    "save" in text.lower()
                                                ):
                                                    dropdown_items.append(element)
                                                    print(f"[{sid}] Found potential dropdown item: '{text.strip()}'")
                                            except:
                                                continue
                                    
                                    print(f"[{sid}] Final count: {len(dropdown_items)} dropdown items for {prospect_name}")
                                    
                                    # Debug: Print all available dropdown options
                                    if dropdown_items:
                                        print(f"[{sid}] Available dropdown options:")
                                        for i, dd_item in enumerate(dropdown_items):
                                            try:
                                                item_text = (await dd_item.inner_text()).strip()
                                                print(f"[{sid}]   Option {i+1}: '{item_text}'")
                                            except:
                                                print(f"[{sid}]   Option {i+1}: [Unable to read text]")
                                    
                                    target_list_found = False
                                    if dropdown_items:
                                        # First, try exact match
                                        for dd_item in dropdown_items:
                                            try:
                                                item_text = (await dd_item.inner_text()).strip()
                                                if item_text.lower() == downloadFileName.strip().lower():
                                                    print(f"[{sid}] EXACT MATCH - Clicking on dropdown item: '{item_text}'")
                                                    await dd_item.click()
                                                    target_list_found = True
                                                    prospects_added_count += 1
                                                    emailcount += 1
                                                    
                                                    socketio.emit("progress_update", {
                                                        "domain": domain_str_item,
                                                        "message": f"Added {prospect_name} to list '{item_text}' ({prospects_added_count}/{required_count_for_designation})"
                                                    }, room=sid)
                                                    
                                                    await asyncio.sleep(2)
                                                    break
                                            except Exception as e_dropdown:
                                                print(f"[{sid}] Error with exact match dropdown item: {e_dropdown}")
                                                continue
                                        
                                        # If no exact match, try partial match
                                        if not target_list_found:
                                            for dd_item in dropdown_items:
                                                try:
                                                    item_text = (await dd_item.inner_text()).strip()
                                                    if downloadFileName.strip().lower() in item_text.lower():
                                                        print(f"[{sid}] PARTIAL MATCH - Clicking on dropdown item: '{item_text}'")
                                                        await dd_item.click()
                                                        target_list_found = True
                                                        prospects_added_count += 1
                                                        emailcount += 1
                                                        
                                                        socketio.emit("progress_update", {
                                                            "domain": domain_str_item,
                                                            "message": f"Added {prospect_name} to list '{item_text}' ({prospects_added_count}/{required_count_for_designation})"
                                                        }, room=sid)
                                                        
                                                        await asyncio.sleep(2)
                                                        break
                                                except Exception as e_dropdown:
                                                    print(f"[{sid}] Error with partial match dropdown item: {e_dropdown}")
                                                    continue
                                        
                                        # If still no match, try first available list (fallback)
                                        if not target_list_found and dropdown_items:
                                            try:
                                                first_item = dropdown_items[0]
                                                first_item_text = (await first_item.inner_text()).strip()
                                                if first_item_text and "create" not in first_item_text.lower():
                                                    print(f"[{sid}] FALLBACK - Using first available list: '{first_item_text}'")
                                                    await first_item.click()
                                                    target_list_found = True
                                                    prospects_added_count += 1
                                                    emailcount += 1
                                                    
                                                    socketio.emit("progress_update", {
                                                        "domain": domain_str_item,
                                                        "message": f"Added {prospect_name} to list '{first_item_text}' (fallback) ({prospects_added_count}/{required_count_for_designation})"
                                                    }, room=sid)
                                                    
                                                    await asyncio.sleep(2)
                                            except Exception as e_fallback:
                                                print(f"[{sid}] Error with fallback dropdown selection: {e_fallback}")
                                    
                                    if not target_list_found:
                                        print(f"[{sid}] Could not select any list for {prospect_name}")
                                        print(f"[{sid}] Target list '{downloadFileName}' not found in dropdown options")
                                        # Close dropdown if it's still open
                                        await page.keyboard.press("Escape")
                                        await asyncio.sleep(1)
                                else:
                                    print(f"[{sid}] 'Add to list' button not found for {prospect_name}")
                            else:
                                print(f"[{sid}] Skipping {prospect_name} - doesn't meet preferences")
                                
                        except Exception as e_row:
                            print(f"[{sid}] Error processing row {row_index}: {e_row}")
                            continue
                    
                    print(f"[{sid}] Individual processing complete: {prospects_added_count}/{required_count_for_designation} prospects added to list for '{title_str_item}'")
                    
                    # Set flags only if we actually processed prospects
                    if prospects_added_count > 0:
                        found_flag_domain = True
                        found2_flag_domain = True
                        print(f"[{sid}] SUCCESS: Added {prospects_added_count}/{required_count_for_designation} prospects to list '{downloadFileName}' for designation '{title_str_item}'")
                        
                        # Update num_results to track remaining capacity across all designations
                        num_results -= prospects_added_count
                        print(f"[{sid}] Remaining email capacity: {num_results}")
                    else:
                        print(f"[{sid}] WARNING: No prospects were added to the list for '{title_str_item}' - check filtering criteria")
                    
                except Exception as e_individual_processing:
                    print(f"[{sid}] Error during individual processing: {e_individual_processing}")
                    print(f"[{sid}] Falling back to original row processing method...")
                    
                    # Fallback to original processing method
                    heap_processing_rows = await page.query_selector_all("table[data-v-4f3d4661] tbody tr")
                    if not heap_processing_rows:
                        heap_processing_rows = await page.query_selector_all("table.table tbody tr")
                    if not heap_processing_rows:
                        heap_processing_rows = await page.query_selector_all("tbody tr")

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
              
                for r_h_idx, r_h_item in enumerate(heap_processing_rows):
                    await check_cancel(state, f"Building heap for {domain_str_item}/{title_str_item}, row {r_h_idx}")
                    try:
                        # Name is in the 2nd column (index 1), job title might be in the same cell or separate
                        name_cell_el = await r_h_item.query_selector("css=td:nth-child(2)")
                        designation_text_lower = (await name_cell_el.inner_text()).strip().lower() if name_cell_el else ""
                        
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

                        span_el = await row_el_h.query_selector('css=td:nth-child(5) .add-list__btn span, td:nth-child(5) button[disabled] span')
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
                        # Email is in the 4th column (index 3) based on the table structure
                        email_cell_el_h_current = await current_row_el_handle.query_selector("css=td:nth-child(4)")
                        if not email_cell_el_h_current or not await email_cell_el_h_current.is_visible():
                            print(f"[{sid}]   Email cell missing or not visible. Skipping row.")
                            continue

                        initial_email_cell_text = (await email_cell_el_h_current.inner_text()).strip()
                        # More robust check for green/yellow status - check in email cell
                        gy_selector = "css=span.email-status-1, span.email-status-2, .email-verified, .email-valid"
                        is_green_or_yellow_email_row = bool(await email_cell_el_h_current.query_selector(gy_selector)) if email_cell_el_h_current else False
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

                                # Check if already saved to the target list - Lists column is 5th (index 4)
                                action_cell_el_h = await current_row_el_handle.query_selector("css=td:nth-child(5)")
                                saved_btn_el_h = None
                                if action_cell_el_h:
                                    saved_btn_el_h = await action_cell_el_h.query_selector("xpath=.//button[contains(@class, 'add-list__btn')][normalize-space(.)='Saved' or .//span[normalize-space(.)='Saved']]")

                                if saved_btn_el_h:
                                    try: 
                                        await page.evaluate("el => el.click()", saved_btn_el_h) # Click 'Saved' to open dropdown
                                        await asyncio.sleep(0.7) # Wait for dropdown
                                        
                                        
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
                            # Lists column is 5th (index 4)
                            action_cell_el_h = await current_row_el_handle.query_selector("css=td:nth-child(5)")
                            add_list_btn_el_h = None
                            if action_cell_el_h:
                               
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
                        
                        try: # Extract other details
                            name_from_row = "N/A"
                            jobtitle_from_row = "N/A"
                            company_from_row = "N/A"
                            location_from_row = "N/A"
                            
                            # Extract name using updated selectors
                            name_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--name a")
                            if name_el_h_val: 
                                name_from_row = (await name_el_h_val.inner_text()).strip()
                            else: # Fallback if 'a' tag not present
                                name_cell_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--name")
                                if name_cell_el_h_val: 
                                    name_from_row = (await name_cell_el_h_val.inner_text()).split('\n')[0].strip()
                            
                            # Extract job title
                            jobtitle_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--name > div")
                            if jobtitle_el_h_val: 
                                jobtitle_from_row = (await jobtitle_el_h_val.inner_text()).strip()
                            
                            # Extract company
                            company_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-name a")
                            if company_el_h_val: 
                                company_from_row = (await company_el_h_val.inner_text()).strip()
                            else:
                                company_cell_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-name")
                                if company_cell_el_h_val: 
                                    company_from_row = (await company_cell_el_h_val.inner_text()).strip().split('\n')[0].strip()

                            # Extract location
                            location_el_h_val = await current_row_el_handle.query_selector("css=div.row__cell--company-country")
                            if location_el_h_val: 
                                location_from_row = (await location_el_h_val.inner_text()).strip()
                        except Exception as e_details_extract:
                            print(f"[{sid}]     Error extracting details for row: {e_details_extract}")

                        # Add the email data to file_data for saving
                        file_data.append({
                            "First Name": name_from_row, 
                            "job title": jobtitle_from_row,
                            "company": company_from_row, 
                            "location": location_from_row,
                            "email": final_email_str_from_row, 
                            "domain": domain_str_item
                        })
                        
                        # Also add to preview list for real-time display
                        state["preview_list"].append({
                            "First Name": name_from_row, 
                            "job title": jobtitle_from_row,
                            "company": company_from_row, 
                            "location": location_from_row,
                            "email": final_email_str_from_row, 
                            "domain": domain_str_item
                        })
                        
                        # Emit preview update
                        try:
                            socketio.emit("preview_update", {"data": state["preview_list"]}, room=sid)
                        except Exception as e_emit_preview:
                            print(f"[{sid}] Error emitting preview update: {e_emit_preview}")
                        
                        print(f"[{sid}]   EMAIL SAVED: {final_email_str_from_row} - {name_from_row} at {company_from_row}")
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
            
            # After all designations for a domain
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
        
        # Keep browser open for debugging - comment out the close section
        print(f"[{sid}] Browser context kept open for debugging...")
        # if state.get("current_context"):
        #     try:
        #         print(f"[{sid}] Closing browser context...")
        #         await state["current_context"].close()
        #         print(f"[{sid}] Browser context closed successfully.")
        #     except Exception as e_close:
        #         print(f"[{sid}] Error closing browser context: {e_close}")
        #     finally:
        #         state["current_context"] = None
        #         state["current_page"] = None

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

#Health 
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok", "message": "API is running"}), 200


# --- Start the worker thread ---
def start_playwright_worker_thread():
    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(playwright_worker_main())
        except KeyboardInterrupt:
            print("Worker thread interrupted. Shutting down...")
        finally:
            # Ensure remaining tasks are cancelled if loop is closing
            # This is complex; proper shutdown needs tasks to be awaited or cancelled.
            # For now, playwright_worker_main handles its own cleanup.
            loop.close()
            print("Worker thread event loop closed.")

    thread = threading.Thread(target=run_loop, daemon=True, name="PlaywrightWorkerThread")
    thread.start()
    print("Playwright worker thread started.")
    return thread

worker_thread = start_playwright_worker_thread()


# ───────────── 3) SOCKETIO EVENT HANDLERS ───────────────────────────────────
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
        "current_context": None, # Will be set by handle_one_job
        "current_page": None,    # Will be set by handle_one_job
    }
    print(f"Client connected: {sid}")

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    print(f"Client disconnected: {sid}")
    state = clients_state.get(sid)
    if state:
        state["cancel_process"] = True # Signal cancellation to any running job
        # The actual context closing is best handled by check_cancel in handle_one_job
        # Forcibly closing from here can be problematic due to thread issues.
        # The example's approach is to set the flag.
        if state.get("current_context"):
            print(f"[{sid}] Disconnect: current_context exists. Cancellation flag set. Worker will handle close.")
            # Attempting a direct close from here is risky as context lives in worker thread.
            # loop = WORKER_EVENT_LOOP
            # if loop and state["current_context"]:
            #     # loop.call_soon_threadsafe(state["current_context"].close) # This is still tricky
            #     pass

    clients_state.pop(sid, None)


@socketio.on("process_data")
def handle_process_data(payload):
    sid = request.sid
    state = clients_state.get(sid)
    if not state:
        emit("process_data_response", {"message": "Error: Client state not found. Please reconnect.", "error": "StateNotFound"}, room=sid)
        return

    # Reset cancellation & clear previous results for a new job
    state["cancel_process"] = False
    state["df_list"].clear()
    state["preview_list"].clear()
    # current_context/page are managed by handle_one_job, no need to clear here explicitly unless for sanity.
    state["current_context"] = None
    state["current_page"] = None


    try:
        domains_str = payload.get('domains', '')
        # Using original get_domain, ensure it's defined globally
        domains_list = [get_domain(d) for d in domains_str.splitlines() if d.strip() and get_domain(d)]


        designations_payload = payload.get('designations', '')
        # downloadFileName is used for Snov list name, not actual file download here
        downloadFileName_from_payload = payload.get('downloadFileName', 'Scraped_Prospects_List') 
        
        designations_list_raw = [d.strip() for d in designations_payload.split(',') if d.strip()]
        
        location_payload = payload.get('location', '')
        location_list_parsed = [l.strip().lower() for l in location_payload.split(',') if l.strip()]
        location_list_parsed = [l.replace('  ', ' ') for l in location_list_parsed]


        array_data_str = payload.get('arrayData', '[]')
        num_results_from_payload = int(payload.get('numResults', 10)) # Default to 10 if not provided

        # Cookie parsing
        converted_cookies_list = []
        try:
            # Sanitize boolean string representations for ast.literal_eval
            array_data_str_sanitized = array_data_str.replace("true", "True").replace("false", "False")
            converted_cookies_list = ast.literal_eval(array_data_str_sanitized)
            if not isinstance(converted_cookies_list, list):
                raise ValueError("Cookie data is not a list.")
        except Exception as e_cookie:
            emit('process_data_response', {"message": f"Invalid cookie data format: {e_cookie}", "error": "CookieParsingFailed"}, room=sid)
            return

        # Build required_counts and clean designations list
        required_counts_map = {}
        parsed_designations_list = []
        pattern = re.compile(r'^\s*(.+?)\s*(\d+)\s*$')
        for item_designation in designations_list_raw:
            match = pattern.search(item_designation)
            if match:
                designation_name = match.group(1).strip().lower() # Use lower for consistency
                count = int(match.group(2))
                parsed_designations_list.append(designation_name)
                required_counts_map[designation_name] = count
            else: # No count, just designation name
                parsed_designations_list.append(item_designation.strip().lower())
                # required_counts_map[item_designation.strip().lower()] = float('inf') # Or some default large number / handle in logic


        print(f"[{sid}] Job to queue: Domains: {len(domains_list)}, Designations: {parsed_designations_list}, Locations: {location_list_parsed}, Cookies: {len(converted_cookies_list)}")
        print(f"[{sid}] Required counts: {required_counts_map}")

        # Prepare job tuple for the worker
        job = (
            sid,
            domains_list,
            parsed_designations_list, # Send the cleaned list of designation names
            location_list_parsed,
            converted_cookies_list,
            required_counts_map, # Send the map
            num_results_from_payload,
            downloadFileName_from_payload
        )

        # Wait for worker to be ready if needed
        max_wait = 10  # seconds
        wait_time = 0
        while (playwright_job_queue is None or WORKER_EVENT_LOOP is None) and wait_time < max_wait:
            import time
            time.sleep(0.5)
            wait_time += 0.5
        
        if playwright_job_queue is None or WORKER_EVENT_LOOP is None:
            emit("process_data_response", {"message": "Error: Worker queue not ready after waiting.", "error": "WorkerNotReady"}, room=sid)
            return

        # Enqueue job using the worker's event loop
        WORKER_EVENT_LOOP.call_soon_threadsafe(playwright_job_queue.put_nowait, job)
        
        emit("process_data_response", {"message": "Playwright job queued successfully."}, room=sid)

    except Exception as e_payload:
        print(f"[{sid}] Error processing payload for process_data: {e_payload}")
        import traceback
        traceback.print_exc()
        emit("process_data_response", {"message": f"Error in submitted data: {e_payload}", "error": "PayloadError"}, room=sid)


@app.route('/proxy/stats', methods=['GET'])
def get_proxy_stats():
    """Get proxy rotation statistics"""
    stats = proxy_rotator.get_stats()
    return {"status": "success", "data": stats}

@app.route('/proxy/add', methods=['POST'])
def add_proxy():
    """Add a new proxy to rotation"""
    data = request.get_json()
    proxy_string = data.get('proxy')
    
    if not proxy_string:
        return {"status": "error", "message": "Proxy string required"}, 400
    
    try:
        proxy_rotator.add_proxy(proxy_string)
        return {"status": "success", "message": f"Proxy added: {proxy_string}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/proxy/remove', methods=['POST'])
def remove_proxy():
    """Remove a proxy from rotation"""
    data = request.get_json()
    proxy_string = data.get('proxy')
    
    if not proxy_string:
        return {"status": "error", "message": "Proxy string required"}, 400
    
    try:
        proxy_rotator.remove_proxy(proxy_string)
        return {"status": "success", "message": f"Proxy removed: {proxy_string}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@app.route('/proxy/test', methods=['POST'])
def test_proxy_endpoint():
    """Test a specific proxy"""
    data = request.get_json()
    proxy_string = data.get('proxy')
    
    if not proxy_string:
        return {"status": "error", "message": "Proxy string required"}, 400
    
    try:
        proxy_config = proxy_rotator._parse_proxy(proxy_string)
        # Note: This would need to be made async in a real implementation
        return {"status": "success", "message": "Proxy format valid", "config": proxy_config}
    except Exception as e:
        return {"status": "error", "message": f"Invalid proxy format: {str(e)}"}, 400

@app.route('/proxy/rotate', methods=['POST'])
def rotate_proxy():
    """Force proxy rotation"""
    try:
        new_proxy = proxy_rotator.get_next_proxy()
        return {"status": "success", "message": "Proxy rotated", "new_proxy": new_proxy}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@socketio.on("refresh")
def handle_refresh():
    sid = request.sid
    state = clients_state.get(sid)
    if not state:
        return

    print(f"[{sid}] Refresh request received.")
    state["cancel_process"] = True # Signal cancellation

    # current_context and current_page are primarily managed by handle_one_job's finally block.
    # Setting them to None here is mostly for logical clarity on refresh.
    # The actual cleanup happens in handle_one_job when check_cancel is hit.
    if state.get("current_context"):
         print(f"[{sid}] Refresh: current_context exists. Cancellation flag set. Worker will handle close.")
    
    state["current_context"] = None
    state["current_page"] = None
    state["df_list"].clear()
    state["preview_list"].clear()

    emit("refresh_response", {"message": "Application state refreshed successfully. Ongoing process will be cancelled."}, room=sid)


# ─────────────────────────────────────────────────────────────────────────────
# 4) RUN THE SERVER
#─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Ensure worker thread is robustly started before app runs
    # but `worker_thread = start_playwright_worker_thread()` already does this.
    # A small delay might be useful to ensure the worker loop is up if there are startup races.
    import time
    time.sleep(2) # Wait for worker thread to be ready
    
    socketio.run(
        app,
        host="0.0.0.0",
        port=4000,
        debug=True,
        allow_unsafe_werkzeug=True
    )
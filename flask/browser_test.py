import asyncio
from playwright.async_api import async_playwright

async def test_browser_visibility():
    """Simple test to verify browser window appears"""
    print("Starting browser visibility test...")
    
    async with async_playwright() as p:
        # Launch browser with maximum visibility settings
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox", 
                "--disable-dev-shm-usage", 
                "--window-size=1920,1080",
                "--start-maximized",
                "--new-window",
                "--disable-background-mode",
                "--force-color-profile=srgb"
            ],
            slow_mo=2000,
            devtools=True,
            channel="chrome"  # Use system Chrome
        )
        
        print("Browser launched - you should see a Chrome window now!")
        
        # Create a page
        page = await browser.new_page()
        print("Page created...")
        
        # Navigate to Google
        await page.goto("https://www.google.com")
        print("Navigated to Google - browser window should be visible!")
        
        # Wait for user to confirm
        print("\n" + "="*50)
        print("BROWSER VISIBILITY TEST")
        print("="*50)
        print("Can you see the Chrome browser window with Google homepage?")
        print("If YES: The browser visibility is working")
        print("If NO: There may be a system-level issue")
        print("="*50)
        
        # Keep browser open for 15 seconds
        await asyncio.sleep(15)
        
        print("Closing browser...")
        await browser.close()
        print("Test completed!")

if __name__ == "__main__":
    asyncio.run(test_browser_visibility())

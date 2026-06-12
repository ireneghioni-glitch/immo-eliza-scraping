from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import time
import random
SEARCH_URL = "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre"
TARGET = 10

def get_listing_urls(max_urls=10):
    urls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--start-maximized"])
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="fr-BE",
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        page_num = 1

        while len(urls) < max_urls:
            url = f"{SEARCH_URL}&page={page_num}"
            print(f"\n  Page {page_num} → {url}")

            page.goto(url, wait_until="networkidle", timeout=30000)

            if page_num == 1:
                try:
                    page.click("button#didomi-notice-agree-button", timeout=4000)
                    print("  ✓ Bannière cookies fermée")
                    time.sleep(1)
                except:
                    pass

            for _ in range(5):
                page.mouse.wheel(0, random.randint(400, 700))
                time.sleep(random.uniform(0.5, 1.0))

            page.wait_for_function(
                "document.querySelectorAll('article[data-url]').length > 5",
                timeout=15000
            )

            data_urls = page.eval_on_selector_all(
                "article[data-url]",
                "els => els.map(el => el.getAttribute('data-url'))"
            )
            
            found = 0
            for href in data_urls:
                if href and "/detail/" in href and href not in urls:
                    urls.append(href)
                    found += 1
                    print(f"    ✓ {href}")
                    if len(urls) >= max_urls:
                        break

            print(f"  {found} nouvelles URLs sur cette page")

            if found == 0:
                print("  Aucune annonce /detail/ trouvée, arrêt.")
                break

            page_num += 1
            time.sleep(random.uniform(2, 3))

        browser.close()

    return urls


def main ():
    print("Collecte des URLs Immovlan...\n")
    listing_urls = get_listing_urls(max_urls=TARGET)

    print(f"\n{'='*50}")
    print(f"{len(listing_urls)} URLs collectées :\n")
main()

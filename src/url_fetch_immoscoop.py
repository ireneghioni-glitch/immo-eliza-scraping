from bs4 import BeautifulSoup
import csv
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class UrlFetch:

    def __init__(self, max_pages=4, target=200):
        self.max_pages = max_pages
        self.target = target
        self.all_urls = []
        self.property_types = ["house", "apartment"]
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=options)

    def accept_cookies(self):
        try:
            wait = WebDriverWait(self.driver, 10)
            accept_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAl"]]')))
            accept_button.click()
            print("Cookies accepted!")
            time.sleep(2)
        except:
            pass

    def fetch_urls(self):
        self.driver.get("https://www.immoscoop.be/en/search/for-sale/house")
        time.sleep(3)
        self.accept_cookies()

        for property_type in self.property_types:
            for page_num in range(1, self.max_pages + 1):
                try:
                    base = f"https://www.immoscoop.be/en/search/for-sale/{property_type}"
                    page_suffix = "" if page_num == 1 else f"?page={page_num}"
                    url = base + page_suffix

                    self.driver.get(url)
                    time.sleep(5)
                    self.accept_cookies()

                    soup = BeautifulSoup(self.driver.page_source, "html.parser")
                    links = soup.find_all("a", href=True)

                    for link in links:
                        href = link["href"]
                        if href.startswith("/en/for-sale/"):
                            full_url = "https://www.immoscoop.be" + href
                            if full_url not in self.all_urls:
                                self.all_urls.append(full_url)

                    print(f"[{property_type}] Page {page_num} — Total: {len(self.all_urls)}")

                except Exception as e:
                    print(f"[{property_type}] Page {page_num} failed: {e}")

                if len(self.all_urls) >= self.target:
                    break

            if len(self.all_urls) >= self.target:
                break

        self.driver.quit()

        with open("urls.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url"])
            for url in self.all_urls:
                writer.writerow([url])
        print("URLs saved to urls.csv!")

        return self.all_urls


fetcher = UrlFetch()
urls = fetcher.fetch_urls()
print(f"Total URLs returned: {len(urls)}")
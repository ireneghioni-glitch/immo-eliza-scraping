from threading import Thread, RLock, Semaphore
import requests
from bs4 import BeautifulSoup
import csv
import time

all_urls = []
lock = RLock()

with open("fetched_urls.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["url"])


class SearchSession:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
        self.session = requests.Session()
        self.session.headers.update(self.headers)


class SearchUrls(Thread):

    def __init__(self, url, session, all_urls, semaphore):
        super().__init__()          
        self.url = url             
        self.session = session
        self.all_urls = all_urls
        self.semaphore = semaphore  

    def run(self):                  
        with self.semaphore:        
            self.search_url(self.url)

    def search_url(self, url):
        for page_num in range(1, 21):
            try:
                base_url = url.format(page_num=page_num)
                response = self.session.get(base_url)
                soup = BeautifulSoup(response.text, "html.parser")

                links = soup.find_all("a", href=lambda h: h and "/en/detail/" in h)

                with lock:                                        
                    with open("fetched_urls.csv", "a", newline="") as f:
                        writer = csv.writer(f)
                        for link in links:
                            href = link["href"]
                            if href not in self.all_urls:
                                self.all_urls.append(href)
                                writer.writerow([href])
                                print(href)

                if len(self.all_urls) >= 10000:
                    break

                print(f"Page {page_num} — collected {len(links)} URLs — Total: {len(self.all_urls)}")
                time.sleep(0.5)

            except Exception as e:
                print(f"Page {page_num} failed: {e}")


def build_urls():
    urls = []
    price_ranges = [(0, 100000), (100000, 200000), (200000, 300000),
                    (300000, 400000), (400000, 500000), (500000, None)]
    property_types = {
        "house": ["residence", "villa", "bungalow", "cottage"],
        "apartment": ["apartment", "penthouse", "duplex", "studio"]
    }

    for prop_type, subtypes in property_types.items():
        for subtype in subtypes:
            for min_price, max_price in price_ranges:
                if max_price:
                    url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale&propertytypes={prop_type}&propertysubtypes={subtype}&minprice={min_price}&maxprice={max_price}&noindex=1&page={{page_num}}"
                else:
                    url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale&propertytypes={prop_type}&propertysubtypes={subtype}&minprice={min_price}&noindex=1&page={{page_num}}"
                urls.append(url)
    return urls


def run_scraper(max_concurrent=50):         
    session = SearchSession().session
    semaphore = Semaphore(max_concurrent)  
    urls = build_urls()

    threads = [
        SearchUrls(url, session, all_urls, semaphore)
        for url in urls
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()                          

    print(f"\nDone — {len(all_urls)} total URLs collected")
    return all_urls


if __name__ == "__main__":
    results = run_scraper(max_concurrent=50)
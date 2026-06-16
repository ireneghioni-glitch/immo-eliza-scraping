from threading import Thread, RLock, Semaphore
import requests
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor

all_urls = set()
lock = RLock()
ban_types=['investment-property','office-space','development-site','land','industrial-building','garage','parking','farmland','commercial-building','student-flat','undetermined-property','to-parcel-out-site','industrial-ground','green-zone','wood','recreational-land']

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
        for page_num in range(1, 2):
            try:
                base_url = url.format(page_num=page_num)
                response = self.session.get(base_url)
                soup = BeautifulSoup(response.text, "html.parser")

                links = soup.find_all("a", href=lambda h: h and "/en/detail/" in h)

                if not links:
                     print(f"Page {page_num} — no links found, stopping")
                     break  
                with lock:                                        
                        for link in links:
                            href = link["href"]
                            if href not in self.all_urls and href.split('/')[5] not in ban_types and href.split('/')[4] != "projectdetail" :
                                self.all_urls.add(href)

                print(f"Page {page_num} — collected {len(links)} URLs — Total: {len(self.all_urls)}")
            except Exception as e:
                print(f"Page {page_num} failed: {e}")



def build_urls():
    urls = []
    provinces = [
        "antwerp", "east-flanders", "west-flanders", "flemish-brabant", "limburg",  # Flanders
        "hainaut", "liege", "luxembourg", "namur", "walloon-brabant",                # Wallonia
        "brussels"                                                                    # Brussels
    ]
    price_ranges = [
        (0, 100000), (100000, 200000), (200000, 300000), (300000, 400000),
        (400000, 500000), (500000, 600000), (600000, 700000), (700000, 800000), (800000, None)
    ]

    for province in provinces:
        for min_price, max_price in price_ranges:
            if max_price:
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale& ={province}&minprice={min_price}&maxprice={max_price}&noindex=1&page={{page_num}}"
            else:
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale&provinces={province}&minprice={min_price}&noindex=1&page={{page_num}}"
            urls.append(url)

    return urls

def run_scraper(max_concurrent=50):         
    session = SearchSession().session
    semaphore = Semaphore(max_concurrent)  
    urls = build_urls()

    def task(url):
        worker = SearchUrls(url, session, all_urls,semaphore)
        worker.search_url(url)

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
         executor.map(task, urls)                          

    print(f"\nDone — {len(all_urls)} total URLs collected")
    return all_urls


if __name__ == "__main__":
    results = run_scraper(max_concurrent=50)
from bs4 import BeautifulSoup
import requests
import csv


class UrlFetch:

    def __init__(self, max_pages=5, target=200):
        self.max_pages = max_pages
        self.target = target
        self.all_urls = []
        self.property_types = ["house", "apartment"]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"})
        

    

    def fetch_urls(self):
        
        

        for property_type in self.property_types:
            for page_num in range(1, self.max_pages + 1):
                try:
                    base = f"https://www.immoscoop.be/en/search/for-sale/{property_type}"
                    page_suffix = "" if page_num == 1 else f"?page={page_num}"
                    url = base + page_suffix
                    response = self.session.get(url)  
                    soup = BeautifulSoup(response.text, "html.parser")

                    
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
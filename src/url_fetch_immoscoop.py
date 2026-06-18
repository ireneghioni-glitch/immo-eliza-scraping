"""
immoscoop_url_fetch.py
======================

URL-collection stage for the ImmoScoop scraping pipeline.

Walks ImmoScoop's paginated for-sale search pages (house + apartment) and
harvests individual listing-detail URLs, deduplicating as it goes. The scraper
(:class:`ImmoScoopScraper`) imports :class:`UrlFetch` and feeds the returned
list into its ``run`` method — mirroring how the Immovlan scraper consumes
``run_scraper`` from its own collection module.

Concurrency model
-----------------
Each (property_type, page) pair is fetched on its own pool thread, since the
work is network-bound. Page fetches run in parallel, but the merge/dedup step
is performed on a single thread (the results are consumed in submission order),
so no lock is required and the collected order stays deterministic.
"""

from bs4 import BeautifulSoup
import requests
import csv
from concurrent.futures import ThreadPoolExecutor


class UrlFetch:
    """Collects ImmoScoop listing-detail URLs from paginated search results.

    Fetches every (property_type, page) combination concurrently, keeping only
    hrefs that point at a detail page (``/en/for-sale/...``). Results are merged
    in submission order and deduplicated; collection stops once ``target`` unique
    URLs have been gathered.
    """

    def __init__(self, max_pages=5, target=5000, max_concurrent=30):
        """Initialise the fetcher.

        Args:
            max_pages: Maximum number of pages to request per property type.
            target: Stop merging once this many unique URLs are collected.
            max_concurrent: Maximum number of pages fetched in parallel.
        """
        self.max_pages = max_pages
        self.target = target
        self.max_concurrent = max_concurrent
        self.all_urls = []
        self.property_types = ["house", "apartment"]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"})

    def _fetch_page(self, property_type, page_num):
        """Fetch one search page and return its detail-page URLs.

        Runs on a worker thread. Self-contained and side-effect free with
        respect to shared state: it returns a list and never touches
        ``self.all_urls``, which is what keeps the design lock-free.

        Args:
            property_type: "house" or "apartment".
            page_num: 1-based page number.

        Returns:
            A list of absolute ImmoScoop detail URLs found on the page (may
            contain duplicates; deduplication happens during the merge). Returns
            an empty list if the request fails.
        """
        try:
            base = f"https://www.immoscoop.be/en/search/for-sale/{property_type}"
            # Page 1 has no query suffix; subsequent pages use ?page=N.
            page_suffix = "" if page_num == 1 else f"?page={page_num}"
            url = base + page_suffix
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # Keep only detail-page links and absolutise them.
            page_urls = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if href.startswith("/en/for-sale/"):
                    page_urls.append("https://www.immoscoop.be" + href)
            return page_urls
        except Exception as e:
            print(f"[{property_type}] Page {page_num} failed: {e}")
            return []

    def fetch_urls(self):
        """Scrape listing URLs across all property types and pages concurrently.

        Writes the collected URLs to ``urls.csv`` and returns them.

        Returns:
            A list of absolute ImmoScoop detail-page URLs (deduplicated).
        """
        # Full task grid: every property type × every page.
        tasks = [
            (property_type, page_num)
            for property_type in self.property_types
            for page_num in range(1, self.max_pages + 1)
        ]

        seen = set()  # O(1) membership for dedup; mirrors ``all_urls`` contents

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit every page fetch up front so they run in parallel...
            futures = [executor.submit(self._fetch_page, pt, pn) for pt, pn in tasks]

            # ...then consume results in submission order. Single-threaded merge
            # means no lock is needed and the output order is stable.
            for (property_type, page_num), future in zip(tasks, futures):
                for full_url in future.result():
                    if full_url not in seen:
                        seen.add(full_url)
                        self.all_urls.append(full_url)

                print(f"[{property_type}] Page {page_num} — Total: {len(self.all_urls)}")

                # Stop merging once the target is met. Already-submitted fetches
                # may still finish, but we simply ignore their results.
                if len(self.all_urls) >= self.target:
                    break

        with open("urls.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url"])
            for url in self.all_urls:
                writer.writerow([url])
        print("URLs saved to urls.csv!")
        return self.all_urls


if __name__ == "__main__":
    fetcher = UrlFetch()
    urls = fetcher.fetch_urls()
    print(f"Total URLs returned: {len(urls)}")
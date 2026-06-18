"""
scrapping_thread.py
===================

URL-collection stage for the immovlan.be scraping pipeline.

This module performs the *discovery* half of the pipeline: it walks immovlan's
paginated search-result pages and harvests the detail-page URLs of individual
listings. The resulting set of URLs is then consumed by the parsing stage
(``PropertyParser`` / ``PropertyScraper`` in the companion module), which fetches
and structures each listing.

Strategy
--------
A naive "fetch every result page" approach hits immovlan's pagination cap, so
the search space is partitioned instead: the cartesian product of **province ×
price band** yields many narrower queries, each returning a manageable number of
pages. ``build_urls`` constructs these query URLs and ``SearchUrls`` scrapes the
listing links out of each one.

Concurrency model
-----------------
Discovery is I/O-bound, so requests are fanned out across a
``ThreadPoolExecutor``. A shared ``set`` (``all_urls``) accumulates results and
is guarded by a module-level ``RLock`` to keep concurrent ``add`` calls safe and
deduplicated.

Filtering
---------
Two filters are applied while harvesting links:

* ``ban_types`` — listing categories that are out of scope (land, garages,
  offices, investment vehicles, …) are dropped based on a URL path segment.
* project pages (``/projectdetail/``) are excluded, since they are aggregate
  developments rather than individual sellable units.
"""

from threading import Thread, RLock, Semaphore
import requests
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor


# ── Shared state ─────────────────────────────────────────────────────────────

# Global, deduplicated sink for every discovered detail URL. Shared across all
# worker threads and guarded by ``lock`` on every mutation.
all_urls = set()
lock = RLock()

# Listing-category slugs that are out of scope for this pipeline. Matched against
# a path segment of each candidate URL so these property kinds never enter the
# result set.
ban_types = ['investment-property','office-space','development-site','land','industrial-building','garage','parking','farmland','commercial-building','student-flat','undetermined-property','to-parcel-out-site','industrial-ground','green-zone','wood','recreational-land']


# ── HTTP session ─────────────────────────────────────────────────────────────

class SearchSession:
    """Thin wrapper that builds a pre-configured ``requests`` session.

    Centralises the browser-like ``User-Agent`` and connection reuse so every
    search request shares the same identity and keep-alive pool.
    """

    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
        self.session = requests.Session()
        self.session.headers.update(self.headers)


# ── Search worker ────────────────────────────────────────────────────────────

class SearchUrls(Thread):
    """Worker that scrapes listing URLs from one paginated search query.

    Subclasses ``Thread`` so it *can* be run as a standalone thread whose
    concurrency is bounded by a semaphore (see :meth:`run`). In the current
    ``run_scraper`` path the work is instead dispatched through a
    ``ThreadPoolExecutor``, which calls :meth:`search_url` directly.
    """

    def __init__(self, url, session, all_urls, semaphore):
        """Initialise a search worker.

        Args:
            url: A search-results URL template containing a ``{page_num}``
                placeholder.
            session: Shared ``requests.Session`` used for the HTTP calls.
            all_urls: Shared result set to populate with discovered URLs.
            semaphore: Concurrency limiter applied when run via :meth:`run`.
        """
        super().__init__()
        self.url = url
        self.session = session
        self.all_urls = all_urls
        self.semaphore = semaphore

    def run(self):
        """Thread entry point: scrape under the semaphore's concurrency cap.

        Only used when the worker is started as a real ``Thread``
        (``worker.start()``). The semaphore bounds how many such workers run
        the network section at once.
        """
        with self.semaphore:
            self.search_url(self.url)

    def search_url(self, url):
        """Fetch each result page of ``url`` and collect its listing links.

        For every page, anchors pointing at ``/en/detail/`` are extracted and,
        after passing the ``ban_types`` and project-page filters, added to the
        shared result set. Iteration stops early as soon as a page yields no
        links, which marks the end of results for that query.

        Args:
            url: Search-results URL template containing ``{page_num}``.
        """
        # NOTE: range(1, 2) only ever requests page 1. Widen the upper bound to
        # paginate further once you're past testing.
        for page_num in range(1, 2):
            try:
                base_url = url.format(page_num=page_num)
                response = self.session.get(base_url)
                soup = BeautifulSoup(response.text, "html.parser")

                # Keep only anchors that point at an individual listing detail.
                links = soup.find_all("a", href=lambda h: h and "/en/detail/" in h)

                # An empty page means we've run past the last result for this
                # query — stop paginating rather than fetching empty pages.
                if not links:
                     print(f"Page {page_num} — no links found, stopping")
                     break
                # Hold the lock while mutating the shared set so concurrent
                # workers can't corrupt it or double-insert.
                with lock:
                        for link in links:
                            href = link["href"]
                            # Skip if: already seen, a banned category (path
                            # segment 5), or a project aggregate page (segment 4).
                            if href not in self.all_urls and href.split('/')[5] not in ban_types and href.split('/')[4] != "projectdetail" :
                                self.all_urls.add(href)

                print(f"Page {page_num} — collected {len(links)} URLs — Total: {len(self.all_urls)}")
            except Exception as e:
                # Per-page isolation: a single failed page is logged and skipped
                # so the rest of the query (and other workers) keep going.
                print(f"Page {page_num} failed: {e}")


# ── Query construction ───────────────────────────────────────────────────────

def build_urls():
    """Build the full set of search-query URL templates to scrape.

    Partitions the search space as **province × price band** so that no single
    query exceeds immovlan's pagination limits. Each returned URL contains a
    ``{page_num}`` placeholder for :meth:`SearchUrls.search_url` to fill in.

    Returns:
        A list of search-results URL templates (one per province/price-band
        combination).
    """
    urls = []
    # All 11 Belgian provinces (incl. the Brussels region) as immovlan slugs.
    provinces = [
        "antwerp", "east-flanders", "west-flanders", "flemish-brabant", "limburg",  # Flanders
        "hainaut", "liege", "luxembourg", "namur", "walloon-brabant",                # Wallonia
        "brussels"                                                                    # Brussels
    ]
    # Price bands in EUR. The final band has no upper bound (open-ended) and is
    # handled by the ``else`` branch below.
    price_ranges = [
        (0, 100000), (100000, 200000), (200000, 300000), (300000, 400000),
        (400000, 500000), (500000, 600000), (600000, 700000), (700000, 800000), (800000, None)
    ]

    for province in provinces:
        for min_price, max_price in price_ranges:
            if max_price:
                # WARNING: this branch's query string is malformed — the
                # province parameter is "& =" instead of "&provinces=", so the
                # province filter is dropped for all bounded price bands. The
                # ``else`` branch below has the correct "&provinces=" form.
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale& ={province}&minprice={min_price}&maxprice={max_price}&noindex=1&page={{page_num}}"
            else:
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale&provinces={province}&minprice={min_price}&noindex=1&page={{page_num}}"
            urls.append(url)

    return urls


# ── Orchestration ────────────────────────────────────────────────────────────

def run_scraper(max_concurrent=50):
    """Run URL discovery across all queries and return the collected URLs.

    Args:
        max_concurrent: Maximum number of worker threads / in-flight requests.
            Also sizes the (currently unused in this path) semaphore.

    Returns:
        The shared ``all_urls`` set populated with every discovered detail URL.
    """
    session = SearchSession().session
    semaphore = Semaphore(max_concurrent)
    urls = build_urls()

    def task(url):
        """Scrape a single search query (runs on a pool thread)."""
        # NOTE: this constructs a SearchUrls worker but calls search_url
        # directly rather than start()/run(), so the Thread machinery and the
        # ``semaphore`` are bypassed — concurrency is bounded solely by the
        # executor's ``max_workers``. The semaphore argument is vestigial here.
        worker = SearchUrls(url, session, all_urls, semaphore)
        worker.search_url(url)

    # Fan the queries out across the pool; results land in the shared set.
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
         executor.map(task, urls)

    print(f"\nDone — {len(all_urls)} total URLs collected")
    return all_urls


if __name__ == "__main__":
    results = run_scraper(max_concurrent=50)
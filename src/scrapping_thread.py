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
Discovery is I/O-bound, so each search query runs on its own ``Thread``. A
shared ``Semaphore`` caps how many queries perform network I/O at once, and a
shared ``set`` (``all_urls``) accumulates results behind a module-level
``RLock`` to keep concurrent ``add`` calls safe and deduplicated.

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
    """Worker thread that scrapes listing URLs from one paginated search query.

    Subclasses ``Thread`` so each query runs on its own thread. Concurrency is
    bounded by the shared ``semaphore``, which :meth:`run` acquires before doing
    any network work — so many workers can exist while only a capped number
    scrape at once.
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

        Invoked by ``Thread.start()``. Acquires the shared semaphore so that no
        more than ``max_concurrent`` workers run the network section at once;
        the permit is released automatically when the ``with`` block exits.
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
        # Scrape up to 30 pages per query. The loop still breaks early (below)
        # as soon as a page returns no listings, so narrow queries that have
        # fewer than 30 pages of results won't fetch empty pages needlessly.
        for page_num in range(1, 31):
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

                # Be polite: a short pause between page requests spreads load and
                # lowers the chance of tripping rate limits now that each query
                # may fetch up to 30 pages. Tune or remove to taste.
                time.sleep(0.5)
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
                # Bounded price band: both minprice and maxprice are set.
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale&provinces={province}&minprice={min_price}&maxprice={max_price}&noindex=1&page={{page_num}}"
            else:
                # Open-ended top band: only minprice is set (no upper bound).
                url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale,in-public-sale&provinces={province}&minprice={min_price}&noindex=1&page={{page_num}}"
            urls.append(url)

    return urls


# ── Orchestration ────────────────────────────────────────────────────────────

def run_scraper(max_concurrent=50):
    """Run URL discovery across all queries and return the collected URLs.

    Spawns one :class:`SearchUrls` thread per search query. All threads are
    started immediately, but each one acquires ``semaphore`` inside its
    :meth:`SearchUrls.run` before doing any network work — so no more than
    ``max_concurrent`` queries hit immovlan at the same time, regardless of how
    many query threads exist. This is what makes the semaphore the real
    concurrency governor (rather than a thread pool's worker count).

    Args:
        max_concurrent: Maximum number of queries allowed to perform network
            I/O simultaneously; also the semaphore's permit count.

    Returns:
        The shared ``all_urls`` set populated with every discovered detail URL.
    """
    session = SearchSession().session
    # Permit pool: exactly ``max_concurrent`` threads may be inside the network
    # section at once; the rest block on acquire() until a permit frees up.
    semaphore = Semaphore(max_concurrent)
    urls = build_urls()

    # Build one worker thread per query. There may be many more workers than
    # permits (e.g. 99 queries vs. 50 permits) — that's fine, the semaphore
    # throttles them down to ``max_concurrent`` active at any moment.
    workers = [SearchUrls(url, session, all_urls, semaphore) for url in urls]

    # start() schedules each worker's run(), which acquires the semaphore before
    # scraping. Starting all of them up front lets the semaphore — not the order
    # of submission — decide who runs when.
    for worker in workers:
        worker.start()

    # Block until every worker has finished so we don't return a partial set.
    for worker in workers:
        worker.join()

    print(f"\nDone — {len(all_urls)} total URLs collected")
    return all_urls


if __name__ == "__main__":
    results = run_scraper(max_concurrent=50)
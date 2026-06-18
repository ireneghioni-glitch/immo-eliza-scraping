"""
immovlan_scraper.py
===================

Concurrent property scraper for immovlan.be real-estate listings.

The pipeline takes a collection of listing URLs and, for each one, fetches the
page, extracts structured data from its embedded JSON-LD blocks and HTML detail
rows, derives Belgian geographic metadata (region/province) from the postal
code, and appends the cleaned record to a CSV file.

Architecture
------------
The module is organised as a small set of single-responsibility components:

* ``Converters``        — pure, stateless value-normalisation helpers.
* ``Geography``         — maps Belgian postal codes to region/province.
* ``DeadLetterQueue``   — thread-safe sink for URLs that fail permanently.
* ``PropertyParser``    — fetches and parses a single listing into a dict.
* ``PropertyScraper``   — orchestrates concurrent parsing and CSV output.

Concurrency model
-----------------
Scraping is I/O-bound (network latency dominates), so a ``ThreadPoolExecutor``
is used rather than multiprocessing. Each worker thread owns its own
``PropertyParser`` (and therefore its own HTTP session), which keeps the
network layer free of shared mutable state. The only shared resources are the
results list, the output CSV, and the dead-letter queue file — all guarded by
locks.

Anti-bot considerations
-----------------------
immovlan serves behind anti-bot protection. To reduce blocking we:

* impersonate a real Chrome TLS fingerprint via ``curl_cffi``,
* rotate User-Agent strings per session via ``fake_useragent``,
* honour 429/503 responses with randomised exponential-style back-off.

Output
------
A CSV file whose columns are defined by ``ALL_COLS``. Every record contains
all columns; fields that could not be extracted are written as empty/``None``.
Failures are recorded in ``failed_urls.txt`` and ``scraping_errors.log``.
"""

from bs4 import BeautifulSoup
import json
import re
from threading import RLock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from .scrapping_thread import run_scraper
import random
import time
import os
from curl_cffi import requests as cffi_requests
from fake_useragent import UserAgent
import logging


# ── Constants ────────────────────────────────────────────────────────────────

# Shared User-Agent generator. Instantiated once at import time because building
# the UA database is relatively expensive; ``ua.random`` is cheap thereafter.
ua = UserAgent()

# Maps the human-readable labels shown in immovlan's "details" table to the
# internal/normalised column names used throughout the pipeline and CSV output.
# This is the single source of truth for which detail rows we care about — any
# label not present here is ignored during parsing.
LABEL_MAP = {
    "State of the property": "state_of_the_building",
    "Furnished":             "furnished",
    "Kitchen equipment":     "kitchen_equipped",
    "Garage":                "has_garage",
    "Number of garages":     "parking_count",
    "Number of facades":     "facades",
    "Number of floors":      "floors_total",
    "Floor of appartment":   "floor_number",
    "Garden":                "has_garden",
    "Surface garden":        "garden_area_m2",
    "Terrace":               "has_terrace",
    "Total land surface":    "total_area_m2",
    "Elevator":              "has_elevator",
}

# Column-type groupings drive how each scraped value is normalised in ``parse``.
# A column's membership here determines which ``Converters`` method is applied.
BOOL_COLS  = ["has_garage", "has_garden", "has_terrace", "furnished", "has_elevator"]
FLOAT_COLS = ["garden_area_m2", "total_area_m2"]
INT_COLS   = ["facades", "parking_count", "floors_total"]

# Full, ordered set of CSV columns. Combines the fixed JSON-LD/derived fields
# with every normalised detail-row field from ``LABEL_MAP``. Used both to write
# the CSV header and to guarantee every record is padded to a consistent shape.
ALL_COLS = [
    "property_id", "property_type", "property_subtype", "price", "price_type",
    "living_area_m2", "bedrooms", "bathrooms", "address", "postal_code", "city",
    "latitude","longitude","building_year", "epc_score", "region", "province",
] + list(LABEL_MAP.values())


# ── Logging ──────────────────────────────────────────────────────────────────

# Errors are persisted to disk (not just stdout) so a long unattended run can be
# audited afterwards. Only ERROR and above are recorded to keep the log signal-
# heavy; progress/info is printed to stdout instead.
logging.basicConfig(
    filename="scraping_errors.log",
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

class Converters:
    """Stateless helpers for normalising raw scraped strings into typed values.

    All methods are static and side-effect free, and every method treats
    ``None`` as a valid, pass-through input so callers can apply them to
    possibly-missing fields without guarding each call.
    """

    @staticmethod
    def to_int(value):
        """Coerce a value to ``int``, preserving ``None``.

        Args:
            value: The raw value to convert (e.g. a numeric string).

        Returns:
            The integer value, or ``None`` if ``value`` is ``None``.
        """
        return int(value) if value is not None else None

    @staticmethod
    def to_bool(value):
        """Convert immovlan's "Yes"/"No" detail labels into a 0/1 flag.

        Returns an integer rather than a Python ``bool`` so the value maps
        cleanly to CSV and downstream tabular/SQL consumers.

        Args:
            value: The raw label text, typically "Yes" or "No".

        Returns:
            ``1`` if ``value`` is exactly "Yes", otherwise ``0``.
        """
        return 1 if value == "Yes" else 0

    @staticmethod
    def parse_float(value):
        """Extract the first numeric token from a string and return it as float.

        Useful for fields like "120 m²" where the unit must be stripped.

        Args:
            value: The raw string possibly containing a number.

        Returns:
            The first parsed float, or ``None`` if ``value`` is ``None`` or
            contains no numeric token.
        """
        if value is None:
            return None
        # Pull every digit/decimal run; we only trust the first match because
        # detail strings occasionally append secondary numbers (units, ranges).
        nums = re.findall(r'[\d.]+', value)
        return float(nums[0]) if nums else None


# ── Geography ─────────────────────────────────────────────────────────────────

class Geography:
    """Derive Belgian administrative geography from a postal code.

    Belgian postal codes are allocated in contiguous numeric bands per province,
    which lets us infer both region and province without an external lookup
    table. The ranges below encode that allocation. ``None`` is returned for
    unknown/out-of-range codes rather than raising, so a single bad listing
    never aborts a batch.
    """

    @staticmethod
    def get_region(postal_code):
        """Map a postal code to one of Belgium's three regions.

        Args:
            postal_code: Integer postal code, or ``None``.

        Returns:
            "Brussels", "Wallonia", or "Flanders"; ``None`` if no code given.
        """
        if postal_code is None:
            return None
        if 1000 <= postal_code < 1300:
            return "Brussels"
        elif (1300 <= postal_code < 1500) or (4000 <= postal_code < 8000):
            return "Wallonia"
        else:
            return "Flanders"

    @staticmethod
    def get_province(postal_code):
        """Map a postal code to its Belgian province.

        Note: a few provinces span non-contiguous code bands (e.g. Flemish
        Brabant and Hainaut), which is why some provinces appear in more than
        one branch below.

        Args:
            postal_code: Integer postal code, or ``None``.

        Returns:
            The province name as a string, or ``None`` if the code is missing
            or falls outside all known ranges.
        """
        if postal_code is None:
            return None
        if 1000 <= postal_code < 1300:
            return "Brussels Capital Region"
        elif 1300 <= postal_code < 1500:
            return "Walloon Brabant"
        elif 1500 <= postal_code < 2000:
            return "Flemish Brabant"
        elif 2000 <= postal_code < 3000:
            return "Antwerp"
        elif 3000 <= postal_code < 3500:
            return "Flemish Brabant"
        elif 3500 <= postal_code < 4000:
            return "Limburg"
        elif 4000 <= postal_code < 5000:
            return "Liège"
        elif 5000 <= postal_code < 5690:
            return "Namur"
        elif 6000 <= postal_code < 6600:
            return "Hainaut"
        elif 6600 <= postal_code < 7000:
            return "Luxembourg"
        elif 7000 <= postal_code < 8000:
            return "Hainaut"
        elif 8000 <= postal_code < 9000:
            return "West Flanders"
        elif 9000 <= postal_code < 10000:
            return "East Flanders"
        else:
            return None


# ── Dead-letter queue ────────────────────────────────────────────────────────

class DeadLetterQueue:
    """Thread-safe, append-only record of URLs that failed permanently.

    Each line is written as ``url,reason`` so a failed run can later be
    inspected, retried, or fed back into the scraper. Append mode plus an
    ``RLock`` keeps writes from interleaving across worker threads.
    """

    def __init__(self, file="failed_urls.txt"):
        """Initialise the queue.

        Args:
            file: Path of the dead-letter file to append to.
        """
        self.file = file
        self.lock = RLock()

    def add(self, url, reason):
        """Append a failed URL and its failure reason to the queue.

        Args:
            url: The listing URL that failed.
            reason: Short, machine-friendly failure cause (e.g. "no_data").
        """
        with self.lock:
            with open(self.file, "a") as f:
                f.write(f"{url},{reason}\n")
            print(f"💀 Added to dead letter queue: {url} — {reason}")

    def load(self):
        """Load previously failed URLs for replay.

        Returns:
            A list of URLs (the first comma-separated field of each line).
            Returns an empty list if the file does not yet exist.
        """
        if not os.path.exists(self.file):
            return []
        with open(self.file) as f:
            return [line.split(",")[0] for line in f.readlines()]


# ── Parser ────────────────────────────────────────────────────────────────────

class PropertyParser:
    """Fetches and parses a single immovlan listing into a flat record.

    Each instance owns its own ``curl_cffi`` session that impersonates a real
    Chrome TLS fingerprint and carries a randomised User-Agent. Because workers
    each construct their own parser, sessions are never shared across threads.
    """

    def __init__(self):
        """Create a session with a spoofed Chrome fingerprint and headers."""
        # ``impersonate`` makes the TLS/JA3 fingerprint look like real Chrome,
        # which is the main lever for getting past immovlan's anti-bot layer.
        self.session = cffi_requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "User-Agent": ua.random,                       # rotate per session
            "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",  # plausible BE locale
            "Referer": "https://immovlan.be/",             # look like in-site nav
        })

    def _get_with_retry(self, url, retries=5):
        """GET a URL with status-aware retry and back-off.

        Handles the common failure modes seen when scraping at volume:

        * **404** — treated as permanent; returns immediately (no retry).
        * **503 / 429** — transient throttling; backs off for an increasing,
          randomised interval and retries.
        * **non-200 or suspiciously short body** — likely a soft block or
          partial page; short random pause then retry.

        Args:
            url: The URL to fetch.
            retries: Maximum number of attempts before giving up.

        Returns:
            The successful ``Response`` object, or ``None`` if the URL is a
            permanent 404 or all retries are exhausted.
        """
        for attempt in range(retries):
            try:
                r = self.session.get(url)

                # Permanent failure — the listing is gone; do not waste retries.
                if r.status_code == 404:
                    logger.error(f"404 Not Found — skipping permanently: {url}")
                    return None  # no retry

                # Server overloaded — back off proportionally to attempt number.
                if r.status_code == 503:
                    wait = (attempt + 1) * random.uniform(5, 10)
                    logger.error(f"503 Service Unavailable — retry {attempt+1}/{retries} after {wait:.1f}s: {url}")
                    time.sleep(wait)
                    continue

                # Rate limited — same escalating back-off as 503.
                if r.status_code == 429:
                    wait = (attempt + 1) * random.uniform(5, 10)
                    logger.error(f"429 Rate Limited — retry {attempt+1}/{retries} after {wait:.1f}s: {url}")
                    print(f" Rate limited — waiting {wait:.1f}s ({attempt+1}/{retries}): {url}")
                    time.sleep(wait)
                    continue

                # Any other non-200, or a body too short to be a real listing
                # (a heuristic for soft blocks / error stubs) — brief retry.
                if r.status_code != 200 or len(r.text) < 1000:
                    logger.error(f"Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    print(f" Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    time.sleep(random.uniform(2, 5))
                    continue

                return r

            except Exception as e:
                # Network-level error (timeout, connection reset, …). Pause and
                # let the loop try again until ``retries`` is exhausted.
                logger.error(f"Request exception attempt {attempt+1}/{retries}: {url} — {e}")
                time.sleep(random.uniform(2, 5))

            # NOTE: this block sits inside the for-loop, so it currently returns
            # after the first attempt's exception path. If you intend to exhaust
            # all retries before giving up, dedent it to run after the loop.
            logger.error(f"Giving up after {retries} retries: {url}")
            print(f" Giving up after {retries} retries: {url}")
            return None

    def parse(self, url):
        """Fetch a listing and extract a normalised property record.

        Data is sourced from two places on the page:

        1. **JSON-LD** ``<script>`` blocks — provide the structured core
           (type, price, area, rooms, address, coordinates, year).
        2. **HTML detail rows** (``div.data-row-wrapper``) — provide the
           secondary attributes listed in ``LABEL_MAP``, each normalised
           according to its ``BOOL_COLS`` / ``FLOAT_COLS`` / ``INT_COLS`` group.

        Several fields (subtype, postal code, city) are parsed from the URL
        path itself, which is more reliable than the page body for those.

        Args:
            url: The listing URL to parse.

        Returns:
            A dict keyed by ``ALL_COLS`` (missing fields filled with ``None``),
            or ``None`` if the page could not be fetched or lacks the required
            JSON-LD blocks.
        """
        # Force the English locale so label text matches ``LABEL_MAP`` keys.
        url = url.replace("/fr/", "/en/").replace("/nl/", "/en/")

        r = self._get_with_retry(url, 5)
        if r is None:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        # Collect every JSON-LD block, keyed by its schema.org @type. Malformed
        # blocks are skipped silently so one bad block can't sink the listing.
        blocks = {}
        for script in soup.select("script[type='application/ld+json']"):
            try:
                block = json.loads(script.string)
                blocks[block["@type"]] = block
            except:
                continue

        # The page may describe either a House or an Apartment, and either a
        # sale or a rental; pick whichever variant is present.
        property_block = blocks.get("House") or blocks.get("Apartment")
        action_block   = blocks.get("SellAction") or blocks.get("RentAction")
        geo_block = blocks.get("GeoCoordinates")

        # Without both the property and the transaction block there's nothing
        # meaningful to record — bail out and let the caller route to the DLQ.
        if not property_block or not action_block:
            print(f"Skipping — no JSON-LD found: {url}")
            return None

        # Core record assembled from JSON-LD plus URL-derived fields. The URL
        # path segments are positional: .../<subtype>/.../<postal>/<city>/...
        data = {
            "property_id":      url,
            "property_type":    property_block.get("@type"),
            "property_subtype": url.split("/")[5],
            "price":            action_block.get("price"),
            "price_type":       action_block.get("@type")[:4].lower(),  # "sell"/"rent"
            "living_area_m2":   property_block.get("floorSize", {}).get("value"),
            "bedrooms":         Converters.to_int(property_block.get("numberOfRooms")),
            "bathrooms":        Converters.to_int(property_block.get("numberOfBathroomsTotal")),
            "address":          property_block.get("address", {}).get("streetAddress"),
            "postal_code":      Converters.to_int(url.split("/")[7]),
            "city":             url.split("/")[8],
            "latitude":         geo_block.get("latitude"),
            "longitude":        geo_block.get("longitude"),
            "building_year":    Converters.to_int(property_block.get("yearBuilt")),
        }

        # Walk the HTML detail rows. Each wrapper holds <h4> label / <p> value
        # pairs; we only keep labels present in LABEL_MAP and normalise the
        # value according to the target column's type group.
        for wrapper in soup.find_all("div", class_="data-row-wrapper"):
            for div in wrapper.find_all("div"):
                h4 = div.find("h4")
                p  = div.find("p")
                if h4 and h4.text in LABEL_MAP:
                    col = LABEL_MAP[h4.text]
                    if col in BOOL_COLS:
                        data[col] = Converters.to_bool(p.text)
                    elif col in FLOAT_COLS:
                        data[col] = Converters.parse_float(p.text)
                    elif col in INT_COLS:
                        data[col] = Converters.to_int(p.text)
                    else:
                        data[col] = p.text

        def get_epc(soup, property_block):
            """Extract the EPC/PEB energy rating (A–G, possibly with '+').

            The rating isn't in the JSON-LD, so we fall back to two text
            sources in order of reliability:

            1. the ``twitter:description`` meta tag, then
            2. the free-text ``description`` of the property block.

            Args:
                soup: Parsed page, used to read the meta tag.
                property_block: The JSON-LD property block, used for fallback.

            Returns:
                The energy class string (e.g. "B", "A+"), or ``None`` if absent.
            """
            # Method 1: the social-share meta description often embeds the EPC.
            meta = soup.find("meta", attrs={"name": "twitter:description"})
            if meta:
                m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', meta["content"])
                if m:
                    return m.group(2) if m else None

            # Method 2: fall back to the listing's long-form description.
            description = property_block.get("description", "")
            m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', description)
            return m.group(2) if m else None

        data["epc_score"] = get_epc(soup, property_block)

        # Derive geography from the postal code parsed above.
        data["region"]   = Geography.get_region(data["postal_code"])
        data["province"] = Geography.get_province(data["postal_code"])

        # Guarantee a uniform record shape: every column in ALL_COLS exists,
        # defaulting to None, so CSV rows stay aligned regardless of what the
        # listing happened to provide.
        for col in ALL_COLS:
            if col not in data:
                data[col] = None

        return data


# ── Scraper ───────────────────────────────────────────────────────────────────

class PropertyScraper:
    """Orchestrates concurrent parsing of many listings into a single CSV.

    Spawns a thread pool, dispatches one parse task per URL, and serialises all
    writes (results list, CSV append, console output) behind a shared lock so
    the output file never interleaves rows from different threads. Permanent
    failures are routed to a :class:`DeadLetterQueue`.
    """

    def __init__(self, output_file="properties.csv", max_concurrent=50):
        """Initialise the scraper and write the CSV header.

        Args:
            output_file: Destination CSV path.
            max_concurrent: Maximum number of worker threads / in-flight
                requests. Tune against the target site's tolerance.
        """
        self.output_file  = output_file
        self.max_concurrent = max_concurrent
        self.results      = []
        self.lock         = RLock()
        self._init_csv()
        self.dlq = DeadLetterQueue()

    def _init_csv(self):
        """Create/truncate the output file and write the column header.

        Opening in "w" mode intentionally overwrites any previous run so each
        invocation starts from a clean file.
        """
        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ALL_COLS)
            writer.writeheader()

    def _process_url(self, url, index=None):
        """Parse one URL and append the result, or record the failure.

        Runs inside a worker thread. Successful records are appended both to the
        in-memory results list and the CSV (under the shared lock); failures are
        sent to the dead-letter queue. Exceptions are caught here so that a
        single bad listing never propagates up and kills the pool.

        Args:
            url: The listing URL to process.
            index: The URL's original position in the input list, used purely
                for readable progress output.
        """
        try:
            # Fresh parser → fresh session, keeping threads fully independent.
            parser = PropertyParser()
            data = parser.parse(url)

            if data is None:
                logger.error(f"No data parsed — added to DLQ: {url}")
                self.dlq.add(url, "no_data")
                return

            # Critical section: mutate shared results and append to the CSV.
            # Held briefly and only around the shared state, not the network I/O.
            with self.lock:
                self.results.append(data)
                with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=ALL_COLS)
                    writer.writerow(data)
                print(f"✓ [{index}] {url}")

        except Exception as e:
            # Catch-all so an unexpected parse error degrades to a DLQ entry
            # instead of crashing the worker.
            logger.error(f"Pipeline error: {url} — {e}")
            self.dlq.add(url, str(e))
            print(f"✗ Failed [{index}] {url}: {e}")

    def run(self, urls):
        """Scrape every URL concurrently and return the collected records.

        Args:
            urls: An iterable of listing URLs. Materialised to a list so each
                URL has a stable index for progress reporting.

        Returns:
            The list of successfully parsed property records.
        """
        urls = list(urls)  # materialise for stable indexing
        print(f"Scraping {len(urls)} properties with {self.max_concurrent} threads...")

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks up front; map each future back to its index so we
            # can report which URL failed if ``result()`` re-raises.
            futures = {executor.submit(self._process_url, url, i): i for i, url in enumerate(urls)}

            for future in as_completed(futures):
                index = futures[future]
                try:
                    future.result()
                except Exception as e:
                    # _process_url already handles its own errors; this is a
                    # last-resort guard for anything that escaped it.
                    logger.error(f"Pipeline error at index {index}: {urls[index]} — {e}")

        print(f"\nDone — {len(self.results)} properties scraped → {self.output_file}")
        return self.results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # ``run_scraper`` (from scrapping_thread) collects the listing URLs to
    # process; the integer argument is the number of pages/listings to gather.
    url = run_scraper(50)

    # Write the CSV next to this script regardless of the current working dir.
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.csv")
    scraper = PropertyScraper(output_file=output_path, max_concurrent=50)
    results = scraper.run(list(url))
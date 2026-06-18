"""
immoscoop_scraper.py
====================

Concurrent property scraper for immoscoop.be for-sale listings.

Given a collection of listing-detail URLs (produced by the companion
``url_fetch_immoscoop`` module), this scraper fetches each page, extracts a
normalised property record from its HTML, derives Belgian geographic metadata
from the postal code, and persists the record through a shared
:class:`StateManager` that also provides crash-resume / checkpointing.

Architecture
------------
* ``Converters``        — stateless value-normalisation helpers.
* ``Geography``         — maps Belgian postal codes to region/province.
* ``DeadLetterQueue``   — thread-safe sink for permanently-failed URLs.
* ``ImmoScoopParser``   — fetches and parses a single listing into a dict.
* ``ImmoScoopScraper``  — orchestrates concurrent parsing + state persistence.

This mirrors the structure of the Immovlan scraper and intentionally reuses the
same ``StateManager``, so both pipelines share resume/checkpoint behaviour.

Concurrency model
-----------------
Scraping is I/O-bound, so a ``ThreadPoolExecutor`` fans listing fetches out
across worker threads. Each worker builds its own ``ImmoScoopParser`` (hence its
own HTTP session), keeping the network layer free of shared mutable state.
Persistence is delegated to ``StateManager``, which is assumed to be internally
thread-safe (see the note in ``_process_url``).

Anti-bot considerations
-----------------------
Each session impersonates a real Chrome TLS fingerprint via ``curl_cffi`` and
carries a randomised User-Agent, with 429/503-aware back-off on retries.
"""

from bs4 import BeautifulSoup
import re
import time
import random
import os
from threading import RLock
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from fake_useragent import UserAgent
from curl_cffi import requests as cffi_requests

from .resilience import StateManager  # ← réutilise le même StateManager que l'autre scraper
from .url_fetch_immoscoop import UrlFetch # ← module de collecte d'URLs ImmoScoop

# ── Constants ────────────────────────────────────────────────────────────────

# Shared User-Agent generator; building the UA database once is cheaper than
# per-request. ``ua.random`` is cheap thereafter.
ua = UserAgent()

# Full, ordered set of CSV/record columns. Every emitted record is padded to
# exactly these keys so downstream tabular consumers see a stable schema.
ALL_COLS = [
    "property_id", "property_type", "property_subtype", "price", "price_type",
    "living_area_m2", "bedrooms", "bathrooms", "address", "postal_code", "city",
    "latitude", "longitude", "building_year", "epc_score", "region", "province",
    "state_of_the_building", "furnished", "kitchen_equipped", "has_garage",
    "parking_count", "facades", "floors_total", "floor_number", "has_garden",
    "garden_area_m2", "has_terrace", "total_area_m2", "has_elevator",
]

# Column-type groupings drive which Converter is applied to each detail value.
BOOL_COLS  = ["has_garage", "has_garden", "has_terrace", "furnished", "has_elevator"]
FLOAT_COLS = ["garden_area_m2", "total_area_m2"]
INT_COLS   = ["facades", "parking_count", "floors_total"]

# Maps ImmoScoop's Dutch-language detail labels to internal column names. Only
# labels present here are extracted from the detail table; anything else is
# ignored. (The site is scraped in its Dutch locale, hence the NL keys.)
LABEL_MAP = {
    "Bewoonbare oppervlakte":   "living_area_m2",
    "Aantal slaapkamers":       "bedrooms",
    "Aantal badkamers":         "bathrooms",
    "Perceeloppervlakte":       "total_area_m2",
    "Oppervlakte tuin":         "garden_area_m2",
    "Aantal verdiepingen":      "floors_total",
    "Verdieping":               "floor_number",
    "Aantal gevels":            "facades",
    "Bouwjaar":                 "building_year",
    "Tuin":                     "has_garden",
    "Terras":                   "has_terrace",
    "Lift":                     "has_elevator",
    "Garage":                   "has_garage",
    "Parking":                  "parking_count",
    "Gemeubeld":                "furnished",
    "Staat van het gebouw":     "state_of_the_building",
    "Keuken":                   "kitchen_equipped",
}

# Yes/No → 1/0 lookup covering both Dutch and English spellings.
BOOL_NL = {"Ja": 1, "Nee": 0, "Yes": 1, "No": 0}

# ── Logging ──────────────────────────────────────────────────────────────────

# Errors are persisted to a dedicated file so an unattended run can be audited
# afterwards; progress/info is printed to stdout instead to keep the log clean.
logging.basicConfig(
    filename="scraping_errors_immoscoop.log",
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

class Converters:
    """Stateless helpers for normalising raw scraped strings into typed values.

    Every method treats ``None`` as a valid pass-through input so callers can
    apply them to possibly-missing fields without guarding each call.
    """

    @staticmethod
    def to_int(value):
        """Coerce a value to ``int`` by stripping all non-digit characters.

        Useful for inputs like "3 slaapkamers" or "1.234" where separators and
        unit text must be discarded.

        Args:
            value: The raw value (any type; stringified before parsing).

        Returns:
            The integer value, or ``None`` if ``value`` is ``None`` or no digits
            remain after stripping.
        """
        if value is None:
            return None
        try:
            return int(re.sub(r"[^\d]", "", str(value)))
        except ValueError:
            return None

    @staticmethod
    def to_bool(value):
        """Map a Yes/No label (NL or EN) to a 0/1 flag.

        Returns an int rather than a bool so it maps cleanly to CSV/SQL. Any
        unrecognised value defaults to 0 (i.e. treated as "No"/absent).

        Args:
            value: The raw label text, e.g. "Ja", "Nee", "Yes", "No".

        Returns:
            1 for an affirmative label, otherwise 0.
        """
        return BOOL_NL.get(value, 0)

    @staticmethod
    def parse_float(value):
        """Extract the first numeric token from a string and return it as float.

        Strips surrounding unit text (e.g. "120 m²" → 120.0).

        Args:
            value: The raw value (any type; stringified before parsing).

        Returns:
            The first parsed float, or ``None`` if ``value`` is ``None`` or
            contains no numeric token.
        """
        if value is None:
            return None
        nums = re.findall(r"[\d.]+", str(value))
        return float(nums[0]) if nums else None


class Geography:
    """Derive Belgian region/province from a postal code.

    Belgian postal codes are allocated in contiguous numeric bands per province,
    so region and province can be inferred without an external lookup table.
    ``None`` is returned for unknown/out-of-range codes rather than raising, so a
    single bad listing never aborts a batch.
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
        Brabant and Hainaut), which is why some appear in more than one branch.

        Args:
            postal_code: Integer postal code, or ``None``.

        Returns:
            The province name, or ``None`` if missing / outside all known ranges.
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


class DeadLetterQueue:
    """Thread-safe, append-only record of URLs that failed permanently.

    Each line is ``url,reason`` so a failed run can later be inspected or
    replayed. Append mode plus an ``RLock`` keeps writes from interleaving
    across worker threads.
    """

    def __init__(self, file="failed_urls_immoscoop.txt"):
        """Initialise the queue.

        Args:
            file: Path of the dead-letter file to append to.
        """
        self.file = file
        self.lock = RLock()

    def add(self, url, reason):
        """Append a failed URL and its reason to the queue.

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
            A list of URLs (the first comma-separated field of each line);
            empty if the file does not yet exist.
        """
        if not os.path.exists(self.file):
            return []
        with open(self.file) as f:
            return [line.split(",")[0] for line in f.readlines()]


# ── Parser ────────────────────────────────────────────────────────────────────

class ImmoScoopParser:
    """Fetches and parses a single ImmoScoop listing into a flat record.

    Each instance owns its own ``curl_cffi`` session impersonating Chrome's TLS
    fingerprint, with a randomised User-Agent. Because each worker constructs its
    own parser, sessions are never shared across threads.

    Unlike the Immovlan scraper (which reads structured JSON-LD), ImmoScoop is
    parsed from rendered HTML via a collection of small, single-field extractor
    methods (``_get_*``). These rely on the page's current DOM structure and are
    the parts most likely to need maintenance if the site's markup changes.
    """

    def __init__(self):
        # même stealth que le scraper Immovlan : curl_cffi + UA aléatoire
        self.session = cffi_requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "User-Agent": ua.random,
            "Accept-Language": "fr-BE,fr;q=0.9,nl;q=0.8",
        })

    def _get_with_retry(self, url, retries=5):
        """GET a URL with status-aware retry and back-off.

        Handles the common failure modes seen when scraping at volume:

        * **404** — permanent; returns immediately (no retry).
        * **503 / 429** — transient throttling; backs off for an increasing,
          randomised interval and retries.
        * **non-200 or suspiciously short body** — likely a soft block or
          partial page; short random pause then retry.

        Args:
            url: The URL to fetch.
            retries: Maximum number of attempts before giving up.

        Returns:
            The successful ``Response``, or ``None`` on a permanent 404 or once
            retries are exhausted.
        """
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=15)

                # Permanent failure — the listing is gone; don't waste retries.
                if r.status_code == 404:
                    logger.error(f"404 Not Found — skipping permanently: {url}")
                    return None

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
                # (heuristic for soft blocks / error stubs) — brief retry.
                if r.status_code != 200 or len(r.text) < 1000:
                    logger.error(f"Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    print(f" Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    time.sleep(random.uniform(2, 5))
                    continue

                return r

            except Exception as e:
                # Network-level error (timeout, connection reset, …): pause then
                # let the loop retry.
                logger.error(f"Request exception attempt {attempt+1}/{retries}: {url} — {e}")
                time.sleep(random.uniform(2, 5))

            # NOTE: this block sits inside the for-loop, so after an *exception*
            # it returns on the first attempt instead of exhausting all retries.
            # (The 503/429/bad-response paths above use ``continue`` and so do
            # retry correctly.) Dedent this block to after the loop if you want
            # exceptions to retry as well.
            logger.error(f"Giving up after {retries} retries: {url}")
            print(f"🚫 Giving up after {retries} retries: {url}")
            return None

    def parse(self, url: str):
        """Fetch a listing and assemble its normalised record.

        Pipeline: fetch → build the base record from the ``_get_*`` extractors →
        overlay the detail-table labels → derive geography → pad missing columns.

        Args:
            url: The listing URL to parse.

        Returns:
            A dict keyed by ``ALL_COLS``, or ``None`` if the page could not be
            fetched.
        """
        r = self._get_with_retry(url, retries=5)
        if r is None:
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        data = self._build_base(soup, url)   # core fields from dedicated extractors
        self._fill_labels(soup, data)        # overlay detail-table attributes
        self._fill_geo(data)                 # region/province from postal code
        self._fill_missing(data)             # pad any column not yet set
        return data

    def _build_base(self, soup, url: str) -> dict:
        """Build the core record from the single-field ``_get_*`` extractors.

        Several fields (area, rooms, coordinates, year) are intentionally left
        ``None`` here and may be populated later by :meth:`_fill_labels`.

        Args:
            soup: Parsed listing page.
            url: The listing URL (also used as the stable ``property_id``).

        Returns:
            A partially-populated record dict.
        """
        return {
            "property_id":      url,
            "property_type":    self._get_type(soup),
            "property_subtype": self._get_subtype(soup),
            "price":            self._get_price(soup),
            "price_type":       "sell",          # this pipeline only scrapes for-sale
            "living_area_m2":   None,
            "bedrooms":         None,
            "bathrooms":        None,
            "address":          self._get_address(soup),
            "postal_code":      self._get_postal_code(soup),
            "city":             self._get_city(soup),
            "latitude":         None,
            "longitude":        None,
            "building_year":    None,
            "epc_score":        self._get_epc(soup),
            "region":           None,
            "province":         None,
        }

    def _fill_labels(self, soup, data: dict):
        """Overlay detail-table attributes onto the record.

        Walks every non-empty text node; when the text matches a ``LABEL_MAP``
        key, the *value* is read from the next sibling of the label's parent and
        normalised according to the target column's type group.

        This sibling-walk encodes an assumption about the page layout
        (label and value sit in adjacent siblings); if ImmoScoop restructures
        its detail table, this is the method to revisit.

        Args:
            soup: Parsed listing page.
            data: The record to mutate in place.
        """
        for label_tag in soup.find_all(string=re.compile(r".+")):
            text = label_tag.strip()
            if text not in LABEL_MAP:
                continue
            col = LABEL_MAP[text]
            value_tag = label_tag.find_parent()
            if value_tag is None:
                continue
            sibling = value_tag.find_next_sibling()
            if sibling is None:
                continue
            raw = sibling.get_text(strip=True)

            # Normalise per the column's type group; default to raw text.
            if col in BOOL_COLS:
                data[col] = Converters.to_bool(raw)
            elif col in FLOAT_COLS:
                data[col] = Converters.parse_float(raw)
            elif col in INT_COLS:
                data[col] = Converters.to_int(raw)
            else:
                data[col] = raw

    def _fill_geo(self, data: dict):
        """Populate region/province from the record's postal code.

        Args:
            data: The record to mutate in place.
        """
        pc = data.get("postal_code")
        data["region"]   = Geography.get_region(pc)
        data["province"] = Geography.get_province(pc)

    def _fill_missing(self, data: dict):
        """Pad the record so every ``ALL_COLS`` key is present.

        Guarantees a uniform record shape regardless of which fields a given
        listing happened to provide.

        Args:
            data: The record to mutate in place.
        """
        for col in ALL_COLS:
            if col not in data:
                data[col] = None

    # ── Extracteurs individuels ───────────────────────────────────────────────
    # Each method below extracts one field from the page. They are deliberately
    # small and forgiving (return None rather than raise) so a missing element
    # degrades a single field instead of failing the whole listing.

    def _get_type(self, soup):
        """Infer property type from the page's <h1> heading.

        Returns:
            "Apartment", "House", "Other", or ``None`` if no heading is found.
        """
        h1 = soup.find("h1")
        if not h1:
            return None
        text = h1.get_text(strip=True).lower()
        if "appartement" in text:
            return "Apartment"
        if "huis" in text or "maison" in text:
            return "House"
        return "Other"

    def _get_subtype(self, soup):
        """Extract the property subtype from the "Subtype" detail row.

        Returns:
            The subtype text, or ``None`` if the row is absent.
        """
        tag = soup.find(string=re.compile(r"Subtype"))
        if tag:
            parent = tag.find_parent()
            sibling = parent.find_next_sibling() if parent else None
            if sibling:
                return sibling.get_text(strip=True)
        return None

    def _get_price(self, soup):
        """Extract the price as a float from the first euro amount on the page.

        Normalises European number formatting (thousands ".", decimal ",").

        Caveat: this takes the *first* "€" amount in the document, which is
        assumed to be the asking price. If a listing shows another euro figure
        earlier (e.g. a fee or "from €…"), that would be picked up instead.

        Returns:
            The price as a float, or ``None`` if none found/parseable.
        """
        match = re.search(r"€\s?([\d\.,]+)", soup.get_text(" "))
        if match:
            # "250.000,50" → "250000.50"
            raw = match.group(1).replace(".", "").replace(",", ".")
            try:
                return float(raw)
            except ValueError:
                return None
        return None

    def _get_address(self, soup):
        """Extract the street address from the location anchor.

        Strips the trailing postal code + city so only the street part remains.

        Returns:
            The street address, or ``None`` if the anchor is absent.
        """
        tag = soup.find("a", href="#location")
        if tag:
            full = tag.get_text(strip=True)
            return re.sub(r",?\s*\d{4}\s+\S+.*$", "", full).strip()
        return None

    def _get_postal_code(self, soup):
        """Extract the 4-digit postal code from the location anchor.

        Returns:
            The postal code as an int, or ``None`` if not found.
        """
        tag = soup.find("a", href="#location")
        if tag:
            match = re.search(r"(\d{4})", tag.get_text())
            if match:
                return int(match.group(1))
        return None

    def _get_city(self, soup):
        """Extract the city name (the text following the postal code).

        Returns:
            The city name, or ``None`` if not found.
        """
        tag = soup.find("a", href="#location")
        if tag:
            match = re.search(r"\d{4}\s+([A-Za-zÀ-ÿ\-\s]+)$", tag.get_text(strip=True))
            if match:
                return match.group(1).strip()
        return None

    def _get_epc(self, soup):
        """Extract the EPC energy rating (A–G, optionally with '+').

        Scans the full page text for an "EPC …" mention.

        Returns:
            The energy-class string, or ``None`` if absent.
        """
        match = re.search(r"EPC[- ]?(?:label|score)?\s*:?\s*([A-G][+]*)", soup.get_text(" "), re.IGNORECASE)
        return match.group(1) if match else None


# ── Scraper ───────────────────────────────────────────────────────────────────

class ImmoScoopScraper:
    """Orchestrates concurrent parsing of many listings with state persistence.

    Spawns a thread pool, dispatches one parse task per URL, and persists each
    successful record through the injected :class:`StateManager` (which also
    drives crash-resume via :meth:`StateManager.filter_remaining` and
    checkpointing). Permanent failures are routed to a :class:`DeadLetterQueue`.
    """

    def __init__(self, state_manager, output_file="immoscoop_properties.csv", max_concurrent=30):
        """Initialise the scraper.

        Args:
            state_manager: Shared ``StateManager`` handling persistence, resume,
                and checkpointing. Assumed to be internally thread-safe.
            output_file: Destination path label for the dataset (the actual
                writing is delegated to ``state_manager``).
            max_concurrent: Maximum number of worker threads / in-flight requests.
        """
        self.output_file    = output_file
        self.max_concurrent = max_concurrent
        self.results        = []
        self.lock            = RLock()
        self.dlq             = DeadLetterQueue()
        self.state_manager   = state_manager

    def _process_url(self, url, index=None):
        """Parse one URL and persist the result, or record the failure.

        Runs inside a worker thread. Exceptions are caught here so a single bad
        listing never propagates up and kills the pool; failures go to the DLQ.

        Thread-safety: writes go through ``state_manager``, which is assumed to
        synchronise its own file access — ``self.lock`` is not taken here.
        ``self.results.append`` is safe under CPython's GIL.

        NOTE: ``save_url_checkpoint(index)`` is called as each task *completes*,
        and completion order is non-deterministic under the pool. If the resume
        logic assumes "every index below the checkpoint is done", that invariant
        does not hold here — a higher index can be checkpointed while a lower one
        is still in flight. Verify ``StateManager``'s resume semantics tolerate
        out-of-order indices (e.g. it tracks a completed set, not a high-water
        mark).

        Args:
            url: The listing URL to process.
            index: The URL's position in the input, used for progress output and
                checkpointing.
        """
        try:
            parser = ImmoScoopParser()   # une session par thread, comme l'autre scraper
            data = parser.parse(url)

            if data is None:
                logger.error(f"No data parsed — added to DLQ: {url}")
                self.dlq.add(url, "no_data")
                return

            self.state_manager.save_property_record(data, url)
            self.results.append(data)
            print(f"✓ [{index}] {url}")

            self.state_manager.save_url_checkpoint(index)

        except Exception as e:
            # Catch-all so an unexpected parse error degrades to a DLQ entry
            # instead of crashing the worker.
            logger.error(f"Pipeline error: {url} — {e}")
            self.dlq.add(url, str(e))
            print(f"✗ Failed [{index}] {url}: {e}")

    def run(self, urls):
        """Scrape every not-yet-done URL concurrently and return the records.

        ``StateManager.filter_remaining`` drops URLs already completed in a prior
        run (crash-resume) and yields ``(index, url)`` pairs, so indices stay
        aligned with the original input for checkpointing.

        Args:
            urls: An iterable of listing URLs.

        Returns:
            The list of property records scraped in this run.
        """
        urls = list(urls)  # ordre stable pour les indices
        remaining = self.state_manager.filter_remaining(urls)
        print(f"Scraping {len(remaining)} ImmoScoop properties with {self.max_concurrent} threads...")

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks; map each future back to its index so a re-raised
            # error can be attributed to the right URL.
            futures = {executor.submit(self._process_url, url, i): i for i, url in remaining}

            for future in as_completed(futures):
                index = futures[future]
                try:
                    future.result()
                except Exception as e:
                    # _process_url handles its own errors; this is a last-resort
                    # guard for anything that escaped it.
                    logger.error(f"Pipeline error at index {index}: {urls[index]} — {e}")

        print(f"\nDone — {len(self.results)} ImmoScoop properties scraped → {self.output_file}")
        return self.results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # 1) Collecte des URLs ImmoScoop via le module de fetch dédié.
    #    max_pages / target contrôlent l'ampleur de la collecte.
    fetcher = UrlFetch(max_pages=5, target=200)
    urls = fetcher.fetch_urls()
    print(f"Collected {len(urls)} ImmoScoop URLs to scrape")

    # 2) Mise en place du StateManager (reprise / checkpointing) puis scraping.
    #    Les chemins sont résolus relativement à ce fichier pour être robustes
    #    au répertoire de travail courant.
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "immoscoop_properties.csv")
    state_manager = StateManager(
        csv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "immoscoop_fetched_urls.csv"),
        json_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "immoscoop_checkpoint.json"),
        dataset_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "immoscoop_properties.jsonl"),
    )
    scraper = ImmoScoopScraper(state_manager=state_manager, output_file=output_path, max_concurrent=30)
    results = scraper.run(urls)
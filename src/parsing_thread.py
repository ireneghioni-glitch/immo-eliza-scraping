"""
parsing_thread.py
=================

Parsing stage for the Immovlan scraping pipeline.

Consumes listing-detail URLs (from the ``scrapping_thread`` discovery stage),
fetches each page, extracts a structured record from its embedded JSON-LD and
HTML detail rows, enriches it with Belgian geography (region, province, and the
nearest notable city with distance + prestige flag), and persists each record
through an injected :class:`StateManager` that provides crash-resume and
checkpointing.

A post-processing helper, :meth:`PropertyParser.clean_duplicate`, deduplicates a
finished dataset by (location, area, price).

Architecture
------------
* ``Converters``        — stateless value-normalisation helpers.
* ``Geography``         — postal-code → region/province, plus nearest-city lookup.
* ``DeadLetterQueue``   — thread-safe sink for permanently-failed URLs.
* ``PropertyParser``    — fetches/parses one listing; also hosts clean_duplicate.
* ``PropertyScraper``   — orchestrates concurrent parsing + state persistence.

Concurrency model
-----------------
Scraping is I/O-bound, so a ``ThreadPoolExecutor`` fans listing fetches across
worker threads. Each worker builds its own ``PropertyParser`` (hence its own HTTP
session), keeping the network layer free of shared mutable state. Persistence is
delegated to ``StateManager``.
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from threading import RLock
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from random import randint
import random
import time
import os
from curl_cffi import requests as cffi_requests
from fake_useragent import UserAgent
import logging
import html
import pandas as pd
from math import radians, sin, cos, sqrt, atan2



# ── Constants ────────────────────────────────────────────────────────────────

# Shared User-Agent generator; built once (expensive), cheap per-call thereafter.
ua = UserAgent()

# Maps Immovlan's English detail-table labels to internal column names. Any label
# not present here is ignored during parsing.
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

# Column-type groupings drive which Converter is applied to each detail value.
BOOL_COLS  = ["has_garage", "has_garden", "has_terrace", "furnished", "has_elevator"]
FLOAT_COLS = ["garden_area_m2", "total_area_m2"]
INT_COLS   = ["facades", "parking_count", "floors_total"]

# Full, ordered set of record columns: fixed JSON-LD/derived fields (incl. the
# three nearest-city fields) followed by every normalised detail-row field.
ALL_COLS = [
    "property_id", "property_type", "property_subtype", "price", "price_type",
    "living_area_m2", "bedrooms", "bathrooms", "address", "postal_code", "city",
    "latitude","longitude","building_year", "epc_score", "region", "province","nearby_city",
    "km_from_nearby_city","is_nearby_city_prestigious"
] + list(LABEL_MAP.values())

# Static reference table of Belgian cities for nearest-city lookup. Each entry
# carries coordinates, population, and a "prestigious" flag that feeds the
# fallback rule in get_nearby_city. Hardcoding avoids any geocoding API.
BELGIAN_CITIES = [
    {"city": "Antwerpen", "lat": 51.2194, "lon": 4.4025, "population": 556138, "prestigious": False},
    {"city": "Bruxelles", "lat": 50.8503, "lon": 4.3517, "population": 188737, "prestigious": False},
    {"city": "Gent", "lat": 51.0543, "lon": 3.7174, "population": 265086, "prestigious": False},
    {"city": "Charleroi", "lat": 50.4108, "lon": 4.4446, "population": 206216, "prestigious": False},
    {"city": "Liège", "lat": 50.6326, "lon": 5.5797, "population": 201256, "prestigious": False},
    {"city": "Bruges", "lat": 51.2093, "lon": 3.2247, "population": 119099, "prestigious": False},
    {"city": "Namur", "lat": 50.4669, "lon": 4.8675, "population": 117453, "prestigious": False},
    {"city": "Louvain", "lat": 50.8798, "lon": 4.7005, "population": 104487, "prestigious": False},
    {"city": "Mons", "lat": 50.4542, "lon": 3.9523, "population": 96450, "prestigious": False},
    {"city": "Alost", "lat": 50.9378, "lon": 4.0397, "population": 90150, "prestigious": False},
    {"city": "Malines", "lat": 51.0259, "lon": 4.4775, "population": 90030, "prestigious": False},
    {"city": "La Louvière", "lat": 50.4796, "lon": 4.1888, "population": 81772, "prestigious": False},
    {"city": "Kortrijk", "lat": 50.8278, "lon": 3.2649, "population": 78878, "prestigious": False},
    {"city": "Hasselt", "lat": 50.9307, "lon": 5.3325, "population": 80948, "prestigious": False},
    {"city": "Sint-Niklaas", "lat": 51.1654, "lon": 4.1429, "population": 79567, "prestigious": False},
    {"city": "Ostende", "lat": 51.2154, "lon": 2.9286, "population": 71978, "prestigious": False},
    {"city": "Tournai", "lat": 50.6064, "lon": 3.3886, "population": 70347, "prestigious": False},
    {"city": "Genk", "lat": 50.9656, "lon": 5.4986, "population": 66227, "prestigious": False},
    {"city": "Roeselare", "lat": 50.9456, "lon": 3.1222, "population": 66453, "prestigious": False},
    {"city": "Mouscron", "lat": 50.7434, "lon": 3.2128, "population": 59904, "prestigious": False},
    {"city": "Verviers", "lat": 50.5891, "lon": 5.8631, "population": 56258, "prestigious": False},
    {"city": "Turnhout", "lat": 51.3225, "lon": 4.9447, "population": 47700, "prestigious": False},
    {"city": "Dendermonde", "lat": 51.0289, "lon": 4.1011, "population": 47700, "prestigious": False},
    {"city": "Sint-Truiden", "lat": 50.8175, "lon": 5.1881, "population": 41000, "prestigious": False},
    {"city": "Lokeren", "lat": 51.1058, "lon": 3.9925, "population": 41700, "prestigious": False},
    {"city": "Geel", "lat": 51.1644, "lon": 4.9914, "population": 41200, "prestigious": False},
    {"city": "Waregem", "lat": 50.8917, "lon": 3.4267, "population": 38500, "prestigious": False},
    {"city": "Arlon", "lat": 49.6833, "lon": 5.8167, "population": 30000, "prestigious": False},
    {"city": "De Panne", "lat": 51.0989, "lon": 2.5928, "population": 10000, "prestigious": True},
    {"city": "Knokke-Heist", "lat": 51.3500, "lon": 3.2833, "population": 34000, "prestigious": True},
    {"city": "Waterloo", "lat": 50.7167, "lon": 4.4000, "population": 30000, "prestigious": True},
    {"city": "Lasne", "lat": 50.7167, "lon": 4.4500, "population": 14000, "prestigious": True},
    {"city": "Rhode-Saint-Genèse", "lat": 50.7500, "lon": 4.3667, "population": 18000, "prestigious": True},
    {"city": "Tervuren", "lat": 50.8264, "lon": 4.5169, "population": 21000, "prestigious": True},
    {"city": "Overijse", "lat": 50.7714, "lon": 4.5333, "population": 25000, "prestigious": True},
    {"city": "Uccle", "lat": 50.8014, "lon": 4.3378, "population": 86852, "prestigious": True},
]

# Major cities just across the FR/LU/NL/DE borders. A frontier property may be
# closest to one of these, so they're part of the lookup pool.
BORDER_CITIES = [
    {"city": "Lille", "lat": 50.6322, "lon": 3.0573, "population": 236710, "prestigious": False},
    {"city": "Dunkerque", "lat": 51.0344, "lon": 2.3768, "population": 86600, "prestigious": False},
    {"city": "Reims", "lat": 49.2583, "lon": 4.0317, "population": 182460, "prestigious": False},
    {"city": "Luxembourg", "lat": 49.6116, "lon": 6.1319, "population": 136208, "prestigious": False},
    {"city": "Maastricht", "lat": 50.8514, "lon": 5.6910, "population": 120105, "prestigious": False},
    {"city": "Eindhoven", "lat": 51.4416, "lon": 5.4697, "population": 238326, "prestigious": False},
    {"city": "Breda", "lat": 51.5719, "lon": 4.7683, "population": 184716, "prestigious": False},
    {"city": "Aachen", "lat": 50.7753, "lon": 6.0839, "population": 249070, "prestigious": False},
    {"city": "Köln", "lat": 50.9375, "lon": 6.9603, "population": 1073096, "prestigious": False},
]

# Combined pool the nearest-city search runs against.
ALL_CITIES = BELGIAN_CITIES + BORDER_CITIES

# ── Logging ──────────────────────────────────────────────────────────────────

# Errors are persisted to disk so a long unattended run can be audited later;
# progress/info goes to stdout to keep the log signal-heavy.
logging.basicConfig(
    filename="scraping_errors.log",
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

class Converters:
    """Stateless helpers for normalising raw scraped values.

    Every method treats ``None`` as a valid pass-through input so callers can
    apply them to possibly-missing fields without guarding each call.
    """

    @staticmethod
    def to_int(value):
        """Coerce a value to ``int``, preserving ``None``."""
        return int(value) if value is not None else None

    @staticmethod
    def to_bool(value):
        """Map the string "Yes" to 1, everything else to 0 (int, for CSV/SQL)."""
        return 1 if value == "Yes" else 0

    @staticmethod
    def true_to_bool(value):
        """Map a Python ``True`` to 1, everything else to 0.

        Used for already-boolean inputs (e.g. the prestige flag) where the value
        is a real bool rather than a "Yes"/"No" string.
        """
        return 1 if value == True else 0

    @staticmethod
    def parse_float(value):
        """Extract the first numeric token from a string and return it as float.

        Strips surrounding unit text (e.g. "120 m²" → 120.0).
        """
        if value is None:
            return None
        nums = re.findall(r'[\d.]+', value)
        return float(nums[0]) if nums else None

    @staticmethod
    def clean_html(text):
        """Decode HTML entities in a string (e.g. ``&amp;`` → ``&``).

        Args:
            text: Raw text possibly containing HTML entities, or ``None``.

        Returns:
            The unescaped text, or ``None`` if ``text`` is ``None``.
        """
        if text is None:
            return None
        return html.unescape(text)


# ── Geography ─────────────────────────────────────────────────────────────────

class Geography:
    """Derive Belgian geography from a postal code or coordinates.

    Provides region/province lookups (from postal-code bands) and a nearest-city
    resolver (from coordinates against the static ``ALL_CITIES`` table). ``None``
    is returned for unknown postal codes rather than raising.
    """

    @staticmethod
    def get_region(postal_code):
        """Map a postal code to one of Belgium's three regions.

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
    def get_nearby_city(latitude, longitude, prestige_radius_km=5):
        """Find the closest city to a coordinate, with a prestige fallback.

        Computes great-circle (haversine) distance to every entry in
        ``ALL_CITIES`` and returns the nearest. The prestige rule prevents an
        upmarket municipality from being attached to a property that merely
        happens to be closest to it: if the nearest city is ``prestigious`` but
        lies farther than ``prestige_radius_km``, the search is rerun over
        non-prestigious cities only.

        Static-table based: no API calls, no rate limits, no ban risk.

        Args:
            latitude: Property latitude (numeric or numeric string).
            longitude: Property longitude (numeric or numeric string).
            prestige_radius_km: Max distance at which a prestigious city may win.

        Returns:
            A 3-tuple ``(city_name, distance_km, is_prestigious)``.
        """
        def haversine(lat1, lon1, lat2, lon2):
            """Great-circle distance in km between two lat/lon points."""
            R = 6371  # Earth radius (km)
            la1, lo1, la2, lo2 = map(radians, [float(lat1), float(lon1), lat2, lon2])
            a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
            return round(R * 2 * atan2(sqrt(a), sqrt(1-a)), 1)

        # Nearest city overall.
        closest = min(ALL_CITIES, key=lambda c: haversine(latitude, longitude, c["lat"], c["lon"]))
        distance = haversine(latitude, longitude, closest["lat"], closest["lon"])

        # Prestige guard: only let a prestigious city win if we're actually near
        # it; otherwise fall back to the nearest non-prestigious city.
        if closest["prestigious"] and distance > prestige_radius_km:
            non_prestigious = [c for c in ALL_CITIES if not c["prestigious"]]
            closest = min(non_prestigious, key=lambda c: haversine(latitude, longitude, c["lat"], c["lon"]))
            distance = haversine(latitude, longitude, closest["lat"], closest["lon"])

        return closest["city"], distance, closest["prestigious"]

    @staticmethod
    def get_province(postal_code):
        """Map a postal code to its Belgian province.

        Note: a few provinces span non-contiguous code bands (e.g. Flemish
        Brabant and Hainaut), hence the repeated branches.

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


# ── Dead-letter queue ────────────────────────────────────────────────────────

class DeadLetterQueue:
    """Thread-safe, append-only record of URLs that failed permanently.

    Each line is ``url,reason`` so a failed run can later be inspected or
    replayed. Append mode + ``RLock`` keeps writes from interleaving across
    worker threads.
    """

    def __init__(self, file="failed_urls.txt"):
        self.file = file
        self.lock = RLock()

    def add(self, url, reason):
        """Append a failed URL and its reason to the queue."""
        with self.lock:
            with open(self.file, "a") as f:
                f.write(f"{url},{reason}\n")
            print(f"💀 Added to dead letter queue: {url} — {reason}")

    def load(self):
        """Load previously failed URLs for replay (empty list if no file yet)."""
        if not os.path.exists(self.file):
            return []
        with open(self.file) as f:
            return [line.split(",")[0] for line in f.readlines()]


# ── Parser ────────────────────────────────────────────────────────────────────

class PropertyParser:
    """Fetches and parses a single Immovlan listing into a flat record.

    Each instance owns its own ``curl_cffi`` session impersonating Chrome's TLS
    fingerprint, with a randomised User-Agent. Because each worker constructs its
    own parser, sessions are never shared across threads.

    Also hosts :meth:`clean_duplicate`, a dataset-level post-processing helper
    (it doesn't use instance state and is a ``@staticmethod``).
    """

    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "User-Agent": ua.random,
            "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
            "Referer": "https://immovlan.be/",
        })

    def _get_with_retry(self, url, retries=5):
        """GET a URL with status-aware retry and back-off.

        * **404** — permanent; returns immediately.
        * **503 / 429** — transient throttling; randomised escalating back-off.
        * **non-200 / short body** — likely a soft block; brief pause then retry.

        Returns:
            The successful ``Response``, or ``None`` on a permanent 404 or once
            retries are exhausted.
        """
        for attempt in range(retries):
            try:
                r = self.session.get(url)

                if r.status_code == 404:
                    logger.error(f"404 Not Found — skipping permanently: {url}")
                    return None

                if r.status_code == 503:
                    wait = (attempt + 1) * random.uniform(5, 10)
                    logger.error(f"503 Service Unavailable — retry {attempt+1}/{retries} after {wait:.1f}s: {url}")
                    time.sleep(wait)
                    continue

                if r.status_code == 429:
                    wait = (attempt + 1) * random.uniform(5, 10)
                    logger.error(f"429 Rate Limited — retry {attempt+1}/{retries} after {wait:.1f}s: {url}")
                    print(f" Rate limited — waiting {wait:.1f}s ({attempt+1}/{retries}): {url}")
                    time.sleep(wait)
                    continue

                if r.status_code != 200 or len(r.text) < 1000:
                    logger.error(f"Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    print(f" Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                    time.sleep(random.uniform(2, 5))
                    continue

                return r

            except Exception as e:
                logger.error(f"Request exception attempt {attempt+1}/{retries}: {url} — {e}")
                time.sleep(random.uniform(2, 5))

            # NOTE: this block is inside the for-loop, so after an *exception* it
            # returns on the first attempt instead of exhausting all retries.
            # (503/429/short-body use ``continue`` and do retry.) Dedent to after
            # the loop if you want exceptions to retry as well.
            logger.error(f"Giving up after {retries} retries: {url}")
            print(f"🚫 Giving up after {retries} retries: {url}")
            return None

    def parse(self, url):
        """Fetch a listing and extract a normalised property record.

        Sources: JSON-LD ``<script>`` blocks (structured core fields) and HTML
        ``div.data-row-wrapper`` detail rows. Several fields (subtype, postal
        code, city) are read positionally from the URL path.

        Returns:
            A dict keyed by ``ALL_COLS``, or ``None`` if the page could not be
            fetched or lacks the required JSON-LD blocks.
        """
        # Force English locale so detail labels match LABEL_MAP keys.
        url = url.replace("/fr/", "/en/").replace("/nl/", "/en/")

        r = self._get_with_retry(url, 5)
        if r is None:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        # Collect every JSON-LD block keyed by @type; skip malformed blocks.
        blocks = {}
        for script in soup.select("script[type='application/ld+json']"):
            try:
                block = json.loads(script.string)
                blocks[block["@type"]] = block
            except:
                continue

        property_block = blocks.get("House") or blocks.get("Apartment")
        action_block   = blocks.get("SellAction") or blocks.get("RentAction")
        # Default to {} so the latitude/longitude lookups below return None
        # instead of raising on pages with no GeoCoordinates block.
        geo_block = blocks.get("GeoCoordinates") or {}

        # Without both property and transaction blocks there's nothing useful to
        # record — bail out so the caller routes the URL to the DLQ.
        if not property_block or not action_block:
            print(f"Skipping — no JSON-LD found: {url}")
            return None

        # Core record from JSON-LD + positional URL fields
        # (.../<subtype>/.../<postal>/<city>/...). Address is HTML-unescaped.
        data = {
            "property_id":      url,
            "property_type":    property_block.get("@type"),
            "property_subtype": url.split("/")[5],
            "price":            action_block.get("price"),
            "price_type":       action_block.get("@type")[:4].lower(),  # "sell"/"rent"
            "living_area_m2":   property_block.get("floorSize", {}).get("value"),
            "bedrooms":         Converters.to_int(property_block.get("numberOfRooms")),
            "bathrooms":        Converters.to_int(property_block.get("numberOfBathroomsTotal")),
            "address":          Converters.clean_html(property_block.get("address", {}).get("streetAddress")),
            "postal_code":      Converters.to_int(url.split("/")[7]),
            "city":             url.split("/")[8],
            "latitude":         geo_block.get("latitude"),
            "longitude":        geo_block.get("longitude"),
            "building_year":    Converters.to_int(property_block.get("yearBuilt")),
        }

        # Overlay detail rows: <h4> label / <p> value pairs, normalised per the
        # column's type group.
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
            """Extract the EPC/PEB energy rating (A–G, optionally with '+').

            Tries the ``twitter:description`` meta tag first, then the block's
            ``description`` field.
            """
            meta = soup.find("meta", attrs={"name": "twitter:description"})
            if meta:
                m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', meta["content"])
                if m:
                    return m.group(2) if m else None
            description = property_block.get("description", "")
            m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', description)
            return m.group(2) if m else None

        data["epc_score"] = get_epc(soup, property_block)
        data["region"]   = Geography.get_region(data["postal_code"])
        data["province"] = Geography.get_province(data["postal_code"])

        # Nearest-city enrichment (needs real coordinates). Skip when missing so
        # the haversine call can't fail on float(None); split the returned
        # (city, distance, prestigious) tuple across the three columns.
        if data["latitude"] is not None and data["longitude"] is not None:
            city, distance, prestigious = Geography.get_nearby_city(data["latitude"], data["longitude"])
            data["nearby_city"]                = city
            data["km_from_nearby_city"]        = distance
            data["is_nearby_city_prestigious"] = Converters.true_to_bool(prestigious)
        else:
            data["nearby_city"]                = None
            data["km_from_nearby_city"]        = None
            data["is_nearby_city_prestigious"] = None

        # Guarantee a uniform record shape: every ALL_COLS key present.
        for col in ALL_COLS:
            if col not in data:
                data[col] = None

        return data

    @staticmethod
    def clean_duplicate(file):
        """Deduplicate the finished JSONL dataset and write a cleaned CSV.

        Reads the records the scraper persisted via
        ``StateManager.save_property_record`` (one JSON object per line). Two
        listings are treated as the same property when they share location
        (latitude/longitude), area, and price. Area prefers ``living_area_m2``
        and falls back to ``total_area_m2``; both area and price are rounded so
        trivial formatting differences don't defeat the match. The first row of
        each duplicate group is kept.

        Args:
            file: Path to the input JSONL dataset of scraped property records
                (e.g. ``properties.jsonl``).

        Side effects:
            Writes the deduplicated rows to
            ``./data/cleaned/properties_cleaned.csv`` (creating the directory if
            needed).
        """
        if not os.path.exists(file):
            print(f"Fichier introuvable : {file}")
            return

        # The scraper writes one JSON object per line (JSONL), not CSV.
        if os.path.getsize(file) == 0:
            print(f"Empty dataset, nothing to clean: {file}")
            return
        df = pd.read_json(file, lines=True)
        print(f"Rows before dedup: {len(df)}")

        # Area key: prefer living area, fall back to total land area; round to
        # 0.1 m² so minor formatting differences still match.
        area_key = (
            pd.to_numeric(df["living_area_m2"], errors="coerce")
            .combine_first(pd.to_numeric(df["total_area_m2"], errors="coerce"))
            .round(1)
        )

        # Price key: numeric, rounded to whole euros.
        price_key = pd.to_numeric(df["price"], errors="coerce").round(0)

        # A row is a duplicate if (lat, lon, area, price) matches an earlier row.
        keys = pd.DataFrame({
            "latitude": df["latitude"],
            "longitude": df["longitude"],
            "area": area_key,
            "price": price_key
        })

        is_duplicate = keys.duplicated(keep="first")

        df_cleaned = df[~is_duplicate].copy().reset_index(drop=True)
        print(f"Rows after dedup: {len(df_cleaned)}")

        # Ensure the destination directory exists before writing.
        out_path = "./data/cleaned/properties_cleaned.csv"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df_cleaned.to_csv(out_path, index=False)
        print(f"Done - {len(df_cleaned)} properties cleaned → {out_path}")


# ── Scraper ───────────────────────────────────────────────────────────────────

class PropertyScraper:
    """Orchestrates concurrent parsing of many listings with state persistence.

    Spawns a thread pool, dispatches one parse task per URL, and persists each
    successful record through the injected :class:`StateManager` (which also
    drives crash-resume via ``filter_remaining`` and checkpointing). Permanent
    failures are routed to a :class:`DeadLetterQueue`.
    """

    def __init__(self, state_manager, max_concurrent=50):  # output_file removed: persistence via state_manager
        self.max_concurrent = max_concurrent
        self.results        = []
        self.lock           = RLock()
        self.dlq            = DeadLetterQueue()
        self.state_manager  = state_manager  # ← StateManager instance

    def _process_url(self, url, index=None):
        """Parse one URL and persist the result, or record the failure.

        Runs inside a worker thread. Exceptions are caught here so a single bad
        listing never kills the pool; failures go to the DLQ.

        ``save_url_checkpoint(index)`` fires as each task completes (in
        non-deterministic order), which is safe here because StateManager tracks
        a *set* of completed indices rather than a high-water mark.
        """
        try:
            parser = PropertyParser()   # one session per thread
            data = parser.parse(url)

            if data is None:
                logger.error(f"No data parsed — added to DLQ: {url}")
                self.dlq.add(url, "no_data")
                return

            self.state_manager.save_property_record(data, url)  # persist to jsonl + csv log
            self.results.append(data)
            print(f"✓ [{index}] {url}")

            self.state_manager.save_url_checkpoint(index)  # mark this index as done

        except Exception as e:
            logger.error(f"Pipeline error: {url} — {e}")
            self.dlq.add(url, str(e))
            print(f"✗ Failed [{index}] {url}: {e}")

    def run(self, urls):
        """Scrape every not-yet-done URL concurrently and return the records.

        ``StateManager.filter_remaining`` drops already-completed URLs (resume)
        and yields ``(index, url)`` pairs.

        Returns:
            The list of property records scraped in this run.
        """
        urls = list(urls)  # stable index order
        remaining = self.state_manager.filter_remaining(urls)  # skip already-done indices
        print(f"Scraping {len(remaining)} properties with {self.max_concurrent} threads...")

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {executor.submit(self._process_url, url, i): i for i, url in remaining}

            for future in as_completed(futures):
                index = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Pipeline error at index {index}: {urls[index]} — {e}")

        # __init__, so referencing it here raised AttributeError on completion.
        print(f"\nDone — {len(self.results)} properties scraped")
        return self.results
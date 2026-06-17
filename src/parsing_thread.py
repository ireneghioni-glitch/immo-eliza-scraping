import requests
from bs4 import BeautifulSoup
import json
import re
from threading import RLock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from scrapping_thread import run_scraper
import random
import time
import os
from curl_cffi import requests as cffi_requests
from fake_useragent import UserAgent
import logging


# ── Constants ────────────────────────────────────────────────────────────────
ua = UserAgent()  

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

BOOL_COLS  = ["has_garage", "has_garden", "has_terrace", "furnished", "has_elevator"]
FLOAT_COLS = ["garden_area_m2", "total_area_m2"]
INT_COLS   = ["facades", "parking_count", "floors_total"]

ALL_COLS = [
    "property_id", "property_type", "property_subtype", "price", "price_type",
    "living_area_m2", "bedrooms", "bathrooms", "address", "postal_code", "city",
    "latitude","longitude","building_year", "epc_score", "region", "province",
] + list(LABEL_MAP.values())


# ── Helpers ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="scraping_errors.log",
    level=logging.ERROR,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

class Converters:
    @staticmethod
    def to_int(value):
        return int(value) if value is not None else None

    @staticmethod
    def to_bool(value):
        return 1 if value == "Yes" else 0

    @staticmethod
    def parse_float(value):
        if value is None:
            return None
        nums = re.findall(r'[\d.]+', value)
        return float(nums[0]) if nums else None


# ── Geography ─────────────────────────────────────────────────────────────────

class Geography:
    @staticmethod
    def get_region(postal_code):
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


# ── Parser ────────────────────────────────────────────────────────────────────
class DeadLetterQueue:
    def __init__(self, file="failed_urls.txt"):
        self.file = file
        self.lock = RLock()

    def add(self, url, reason):
        with self.lock:
            with open(self.file, "a") as f:
                f.write(f"{url},{reason}\n")
            print(f"💀 Added to dead letter queue: {url} — {reason}")

    def load(self):
        if not os.path.exists(self.file):
            return []
        with open(self.file) as f:
            return [line.split(",")[0] for line in f.readlines()]

class PropertyParser:
    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome120")  # ← faking real Chrome TLS
        self.session.headers.update({
            "User-Agent": ua.random,
            "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
            "Referer": "https://immovlan.be/",
        })

    def _get_with_retry(self, url, retries=5):
        for attempt in range(retries):
            try:
                r = self.session.get(url)

                if r.status_code == 404:
                    logger.error(f"404 Not Found — skipping permanently: {url}")
                    return None  # no retry

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

            logger.error(f"Giving up after {retries} retries: {url}")
            print(f"🚫 Giving up after {retries} retries: {url}")
            return None

    def parse(self, url):
        url = url.replace("/fr/", "/en/").replace("/nl/", "/en/")

        r = self._get_with_retry(url,5)
        if r is None:
            return None
        soup = BeautifulSoup(r.text, "lxml")

        # JSON-LD blocks
        blocks = {}
        for script in soup.select("script[type='application/ld+json']"):
            try:
                block = json.loads(script.string)
                blocks[block["@type"]] = block
            except:
                continue

        property_block = blocks.get("House") or blocks.get("Apartment")
        action_block   = blocks.get("SellAction") or blocks.get("RentAction")
        geo_block = blocks.get("GeoCoordinates")

        if not property_block or not action_block:
            print(f"Skipping — no JSON-LD found: {url}")
            return None

        data = {
            "property_id":      url,
            "property_type":    property_block.get("@type"),
            "property_subtype": url.split("/")[5],
            "price":            action_block.get("price"),
            "price_type":       action_block.get("@type")[:4].lower(),
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
                        
        #Get the epc
        def get_epc(soup, property_block):
            # Méthode 1 : meta tag
            meta = soup.find("meta", attrs={"name": "twitter:description"})
            if meta:
                m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', meta["content"])
                if m:
                    return m.group(2) if m else None

            # Méthode 2 : description du block
            description =  property_block.get("description", "")
            m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', description)
            return m.group(2) if m else None

        data["epc_score"] = get_epc(soup, property_block)
        data["region"]   = Geography.get_region(data["postal_code"])
        data["province"] = Geography.get_province(data["postal_code"])
       

        # fill missing columns with None
        for col in ALL_COLS:
            if col not in data:
                data[col] = None

        return data


# ── Scraper ───────────────────────────────────────────────────────────────────

class PropertyScraper:
    # IRENE: adding state_manager argument and attribute
    def __init__(self, state_manager, output_file="properties.csv", max_concurrent=50):
        self.output_file  = output_file
        self.max_concurrent = max_concurrent
        self.results      = []
        self.lock         = RLock()
        self._init_csv()
        self.dlq = DeadLetterQueue()
        self.state_manager = state_manager

    def _init_csv(self):
        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ALL_COLS)
            writer.writeheader()

    def _process_url(self, url, index=None):  # ← add index parameter
        try:
            parser = PropertyParser()
            data = parser.parse(url)

            if data is None:
                logger.error(f"No data parsed — added to DLQ: {url}")
                self.dlq.add(url, "no_data")
                return

            # IRENE: integration of save_property_record method from StateManager
            self.state_manager.save_property_record(data, url)
            # with self.lock:
            self.results.append(data)
            #     with open(self.output_file, "a", newline="", encoding="utf-8") as f:
            #         writer = csv.DictWriter(f, fieldnames=ALL_COLS)
            #         writer.writerow(data)
            print(f"✓ [{index}] {url}")  # ← shows index in output

            # IRENE: integration of save_url_checkpoint method from StateManager
            self.state_manager.save_url_checkpoint(index)

        except Exception as e:
            logger.error(f"Pipeline error: {url} — {e}")
            self.dlq.add(url, str(e))
            print(f"✗ Failed [{index}] {url}: {e}")

    def run(self, urls):
        urls = list(urls)  # ← ensures stable index order
        print(f"Scraping {len(urls)} properties with {self.max_concurrent} threads...")

        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {executor.submit(self._process_url, url, i): i for i, url in enumerate(urls)}

            for future in as_completed(futures):
                index = futures[future]  # ← original index of this URL
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Pipeline error at index {index}: {urls[index]} — {e}")

        print(f"\nDone — {len(self.results)} properties scraped → {self.output_file}")
        return self.results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = run_scraper(50)

    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.csv")
    scraper = PropertyScraper(output_file=output_path, max_concurrent=50)
    results = scraper.run(list(url))
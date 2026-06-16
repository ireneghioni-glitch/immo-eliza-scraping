import requests
from bs4 import BeautifulSoup
import json
import re
from threading import RLock
from concurrent.futures import ThreadPoolExecutor
import csv
from scrapping_thread import run_scraper
import random
import time
import os

# ── Constants ────────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]
HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://immovlan.be/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",       # ← browsers always send this
    "Sec-Fetch-Dest": "document",           # ← tells server it's a page request
    "Sec-Fetch-Mode": "navigate",           # ← mimics real navigation
    "Sec-Fetch-Site": "same-origin",        # ← coming from same site
    "Sec-Fetch-User": "?1",                 # ← triggered by user action
    "Cache-Control": "max-age=0",           # ← browser cache behavior
}

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
    "building_year", "epc_score", "region", "province",
] + list(LABEL_MAP.values())


# ── Helpers ──────────────────────────────────────────────────────────────────

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

class PropertyParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_with_retry(self, url, retries=3):          
        for attempt in range(retries):
            r = self.session.get(url)

            if r.status_code == 429:
                wait = (attempt + 1) * random.uniform(5, 10)
                print(f" Rate limited — waiting {wait:.1f}s ({attempt+1}/{retries}): {url}")
                time.sleep(wait)
                continue

            if r.status_code != 200 or len(r.text) < 1000:
                print(f" Bad response {r.status_code} — retry {attempt+1}/{retries}: {url}")
                time.sleep(random.uniform(2, 5))
                continue

            if "captcha" in r.text.lower() or "blocked" in r.text.lower():
                print(f" Block detected — retry {attempt+1}/{retries}: {url}")
                time.sleep(random.uniform(5, 10))
                continue

            return r 

        print(f" Giving up after {retries} retries: {url}")
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
    def __init__(self, output_file="properties.csv", max_concurrent=50):
        self.output_file  = output_file
        self.max_concurrent = max_concurrent
        self.results      = []
        self.lock         = RLock()
        self.parser       = PropertyParser()
        self._init_csv()

    def _init_csv(self):
        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ALL_COLS)
            writer.writeheader()

    def _process_url(self, url):
        try:
            data = self.parser.parse(url)
            if data:
                with self.lock:
                    self.results.append(data)
                    with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=ALL_COLS)
                        writer.writerow(data)
                    print(f"✓ {url}")
        except Exception as e:
            print(f"✗ Failed {url}: {e}")

    def run(self, urls):
        print(f"Scraping {len(urls)} properties with {self.max_concurrent} threads...")
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            executor.map(self._process_url, urls)
        print(f"\nDone — {len(self.results)} properties scraped → {self.output_file}")
        return self.results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = run_scraper(50)

    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.csv")
    scraper = PropertyScraper(output_file=output_path, max_concurrent=50)
    results = scraper.run(list(url))
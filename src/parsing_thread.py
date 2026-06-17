import requests
from bs4 import BeautifulSoup
import json
import re   
import threading
from threading import RLock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import time 
from random import randint
from scrapping_thread import run_scraper
import random
import time
import os
from curl_cffi import requests as cffi_requests
from fake_useragent import UserAgent
import logging

from math import radians, sin, cos, sqrt, atan2

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
    "latitude","longitude","building_year", "epc_score", "region", "province","nearby_city"
] + list(LABEL_MAP.values())

# Belgian cities 
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
    # Popular/Upscale Areas (smaller population but high property values)
    {"city": "De Panne", "lat": 51.0989, "lon": 2.5928, "population": 10000, "prestigious": True},
    {"city": "Knokke-Heist", "lat": 51.3500, "lon": 3.2833, "population": 34000, "prestigious": True},
    {"city": "Waterloo", "lat": 50.7167, "lon": 4.4000, "population": 30000, "prestigious": True},
    {"city": "Lasne", "lat": 50.7167, "lon": 4.4500, "population": 14000, "prestigious": True},
    {"city": "Rhode-Saint-Genèse", "lat": 50.7500, "lon": 4.3667, "population": 18000, "prestigious": True},
    {"city": "Tervuren", "lat": 50.8264, "lon": 4.5169, "population": 21000, "prestigious": True},
    {"city": "Overijse", "lat": 50.7714, "lon": 4.5333, "population": 25000, "prestigious": True},
    {"city": "Uccle", "lat": 50.8014, "lon": 4.3378, "population": 86852, "prestigious": True},
]

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


ALL_CITIES = BELGIAN_CITIES + BORDER_CITIES

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
    def get_nearby_city(latitude, longitude,prestige_radius_km=5):
        """
        Find the closest city (Belgian or major border city) with at least
        min_population inhabitants. Uses a static, hardcoded list — no API
        calls, no rate limits, no ban risk. Instant lookup.
        """

        def haversine(lat1, lon1, lat2, lon2):
            """
            Calculate the great-circle distance (in km) between two GPS points
            using the Haversine formula, which accounts for Earth's curvature.
            """
            R = 6371  # Earth's radius in km
            la1, lo1, la2, lo2 = map(radians, [float(lat1), float(lon1), lat2, lon2])
            a = sin((la2-la1)/2)**2 + cos(la1)*cos(la2)*sin((lo2-lo1)/2)**2
            return round(R * 2 * atan2(sqrt(a), sqrt(1-a)), 1)

        # Step 1: find the closest city overall, regardless of category
        closest = min(ALL_CITIES, key=lambda c: haversine(latitude, longitude, c["lat"], c["lon"]))
        distance = haversine(latitude, longitude, closest["lat"], closest["lon"])

        # Step 2: if the closest city is prestigious but too far away to be
        # meaningful, discard it and search again among non-prestigious cities
        if closest["prestigious"] and distance > prestige_radius_km:
            non_prestigious = [c for c in ALL_CITIES if not c["prestigious"]]
            closest = min(non_prestigious, key=lambda c: haversine(latitude, longitude, c["lat"], c["lon"]))
            distance = haversine(latitude, longitude, closest["lat"], closest["lon"])
            
        return closest["city"], distance, closest["prestigious"]
    
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
        
        
        #HTML parser
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
            # Method 1 : meta tag
            meta = soup.find("meta", attrs={"name": "twitter:description"})
            if meta:
                m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', meta["content"])
                if m:
                    return m.group(2) if m else None

            # Method 2 : in the block's description
            description =  property_block.get("description", "")
            m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', description)
            return m.group(2) if m else None

        data["epc_score"] = get_epc(soup, property_block)
        data["region"]   = Geography.get_region(data["postal_code"])
        data["province"] = Geography.get_province(data["postal_code"])
        data["nearby_city"] = Geography.get_nearby_city(data["latitude"],data["longitude"])
       

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
        self._init_csv()
        self.dlq = DeadLetterQueue()

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

            with self.lock:
                self.results.append(data)
                with open(self.output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=ALL_COLS)
                    writer.writerow(data)
                print(f"✓ [{index}] {url}")  # ← shows index in output

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
import requests
from bs4 import BeautifulSoup
import json
import re


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://immovlan.be/",
    "Connection": "keep-alive",
}

#Label to search in the HTML
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

#Transform to int
def to_int(value):
    return int(value) if value is not None else None

#Transform Yes/No to 1/0
def to_bool(value):
    return 1 if value == "Yes" else 0

#Transform to float
def parse_float(value):
    if value is None:
        return None
    nums = re.findall(r'[\d.]+', value)
    return float(nums[0]) if nums else None

#Get region with postal code
def get_region(postal_code):
    if postal_code is None:
        return None
    if 1000 <= postal_code < 1300:
        return "Brussels"
    elif (1300 <= postal_code < 1500) or (4000 <= postal_code < 8000):
        return "Wallonia"
    else:
        return "Flanders"

#Parser function
def parse_property(url):
    # Force English
    url = url.replace("/fr/", "/en/").replace("/nl/", "/en/")

    r = requests.get(url, headers=HEADERS)
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

    # Base data from JSON-LD
    data = {
        "property_id":      url,
        "property_type":    property_block.get("@type"),
        "property_subtype": url.split("/")[5],
        "price":            action_block.get("price"),
        "price_type":       action_block.get("@type")[:4].lower(),
        "living_area_m2":   property_block.get("floorSize", {}).get("value"),
        "bedrooms":         to_int(property_block.get("numberOfRooms")),
        "bathrooms":        to_int(property_block.get("numberOfBathroomsTotal")),
        "address":          property_block.get("address", {}).get("streetAddress"),
        "postal_code":      to_int(property_block.get("address", {}).get("postalCode")),
        "city":             property_block.get("address", {}).get("addressLocality"),
        "building_year":    to_int(property_block.get("yearBuilt")),
    }

    # Loop to retrieve data from Html with the label list
    for wrapper in soup.find_all("div", class_="data-row-wrapper"):
        for div in wrapper.find_all("div"):
            h4 = div.find("h4")
            p  = div.find("p")
            if h4 and h4.text in LABEL_MAP:
                colonne = LABEL_MAP[h4.text]
                if colonne in BOOL_COLS:
                    data[colonne] = to_bool(p.text)
                elif colonne in FLOAT_COLS:
                    data[colonne] = parse_float(p.text)
                elif colonne in INT_COLS:
                    data[colonne] = to_int(p.text)
                else:
                    data[colonne] = p.text

    # Get the EPC
    meta = soup.find("meta", attrs={"name": "twitter:description"})
    if meta:
        m = re.search(r'(PEB|EPC)\s+([A-G][+]*)', meta["content"])
        data["epc_score"] = m.group(2) if m else None

    # Get Region
    data["region"] = get_region(data["postal_code"])

    # Fill missing columns with None
    all_cols = list(LABEL_MAP.values()) + ["epc_score", "region"]
    for col in all_cols:
        if col not in data:
            data[col] = None

    return data


if __name__ == "__main__":
    urls = [
        "https://immovlan.be/en/detail/villa/a-vendre/6040/jumet/vbe34647",
        "https://immovlan.be/en/detail/maison/a-vendre/6040/jumet/vbe35169",
        "https://immovlan.be/en/detail/penthouse/a-louer/1180/uccle/vbe35168",
        "https://immovlan.be/en/detail/maison/a-vendre/5060/moignelee/vbe35133"
    ]

    for url in urls:
        result = parse_property(url)
        print(result)
        print("---")
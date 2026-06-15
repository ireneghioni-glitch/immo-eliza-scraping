import requests
from bs4 import BeautifulSoup
import json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://immovlan.be/",
    "Connection": "keep-alive",
}

url = 'https://immovlan.be/en/detail/villa/a-vendre/1640/rhode-saint-genese/vbe34924'
r = requests.get(url, headers= headers)
soup = BeautifulSoup(r.text, 'lxml')

#Extract block from json-ld

blocks={}
for script in soup.select("script[type='application/ld+json']"):
    data = json.loads(script.string)
    blocks[data["@type"]] = data

    
property_block = blocks.get("House") or blocks.get("Apartment")
action_block = blocks.get("SellAction") or blocks.get("RentAction")

#Transform str into int
def to_int(value):
    return int(value) if value is not None else None

data = {
    "living_area_m2": property_block.get("floorSize",{}).get("value"),
    "bedrooms":       to_int(property_block.get("numberOfRooms")),
    "bathrooms":      to_int(property_block.get("numberOfBathroomsTotal")),
    "address":        property_block.get("address",{}).get("streetAddress"),
    "postal_code":    to_int(property_block.get("address",{}).get("postalCode")),
    "city":           property_block.get("address",{}).get("addressLocality"),
    "building_year":  to_int(property_block.get("yearBuilt")),
    "price":          action_block.get("price"),
    "price_type":     action_block.get("@type")[:4],
    "property_id":    url,
    "property_type":  property_block.get("@type"),
    "property_subtype" : url.split("/")[5]
}

print(data)
rows = soup.find_all("div", class_="data-row-wrapper")

for wrapper in soup.find_all("div", class_="data-row-wrapper"):
    for div in wrapper.find_all("div"):
        h4 = div.find("h4")
        p  = div.find("p")
        if h4 and p:
            print(h4.text, "->", p.text)
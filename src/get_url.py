import requests
from bs4 import BeautifulSoup
import time

def get_urls(max_pages=25, target= 10000):

 headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
 session = requests.Session()
 session.headers.update(headers) # Persists headers across all requests inthis session

 all_urls = []  # master list to track  URLs and avoid duplicates


# map each main property type to its subtypes for search combinations    

 property_types = {
    "house": ["residence", "villa", "bungalow", "cottage","chalet","mansion","master-house"],
    "apartment": ["apartment", "penthouse", "duplex", "studio","ground-floor","triplex"]
}

# price brackets to divide searches 

 price_ranges = [
    (0, 100000),
    (100000, 200000),
    (200000, 300000),
    (300000, 400000),
    (400000, 500000),
    (500000, 600000),
    (600000, 700000),
    (700000, 800000),
    (800000, None)    # None means no upper limit
]

# build one URL template per (type, subtype, price range) combination
# {page_num} is a placeholder formatted later in the scraping loop
 search_urls = []   
 for prop_type, subtypes in property_types.items():
     for subtype in subtypes:
         for min_price, max_price in price_ranges:
             if max_price:
               url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale&propertytypes={prop_type}&propertysubtypes={subtype}&minprice={min_price}&maxprice={max_price}&noindex=1&page={{page_num}}"
             else:
               url = f"https://immovlan.be/en/real-estate?transactiontypes=for-sale&propertytypes={prop_type}&propertysubtypes={subtype}&minprice={min_price}&noindex=1&page={{page_num}}"    # omit maxprice param entirely when there's no upper bound
             search_urls.append(url)
 print(f"Total search URLs generated: {len(search_urls)}")

 for search_url in search_urls:
  for page_num in range(1, max_pages): #scrape up to 24 pages per search combination
   try:
       base_url = search_url.format(page_num = page_num)
      
       response = session.get(base_url)
       soup = BeautifulSoup(response.text, "html.parser")

       # find all <a> tags whose href contains "/en/detail/" — these are property listing links
       links = soup.find_all("a", href=lambda h: h and "/en/detail/" in h)
      

       for link in links:
        href = link["href"]

        # deduplicate: only save URLs we haven't seen before
        if href not in all_urls:
          all_urls.append(href)
          print(href)

       print(f"Page {page_num} — collected {len(links)} URLs — Total: {len(all_urls)}")
       time.sleep(0.5)

       
       if len(all_urls) >= target: # target reached, stop scraping entirely
           break

   except Exception as e:
    print(f"page number:{page_num} failed :{e}")

 return all_urls

urls = get_urls()
print(f"Total URLs returned: {len(urls)}")



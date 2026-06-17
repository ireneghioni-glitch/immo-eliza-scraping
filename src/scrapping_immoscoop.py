from bs4 import BeautifulSoup
import requests
import csv


class Scrapping_immoscoop:
    def __init__(self):
        with open("urls.csv", "r") as f:
            reader = csv.DictReader(f)
            self.urls = [row["url"] for row in reader]

        self.session = requests.Session()
        self.properties=[]
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"})


    def get_feature(self, soup, label_name):
         # 1. find ALL divs with the label class
        labels = soup.find_all("div", class_="feature-values_component_label__OEv44")
    
        
        for label in labels:
         
         if label.text.strip() == label_name:
             
             value = label.find_next("div", class_="feature-values_component_valueContent__a5NfG")
             return value.text.strip() if value else None
        return None
    

    def scrape_property(self, url):
        response = self.session.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        property_id = url.split("/")[-1]
        price = soup.find("div", class_="heading-2 mb-2").text.strip()
        price = price.replace("€", "").replace(",", "").replace("\xa0", "").strip()
        price = int(price) if price.isdigit() else None
        address_tag = soup.find("a", class_="flex items-center text-gray-600")
        full_address = address_tag.text.strip()
        parts = full_address.split(", ")
        street = parts[0] if len(parts) > 0 else None     
        postal_city = parts[1] if len(parts) > 1 else None
        postal_code = postal_city.split(" ")[0] if postal_city else None
        city = " ".join(postal_city.split(" ")[1:]) if postal_city else None

        bedrooms = self.get_feature(soup, "Number of bedrooms")
        bedrooms = int(bedrooms) if bedrooms else None
        bathrooms = self.get_feature(soup, "Number of bathrooms")
        bathrooms = int(bathrooms) if bathrooms else None
        living_area = self.get_feature(soup, "Surface")
        if living_area:
         living_area = living_area.split(" ")[0].replace (",", ".")  
         living_area = float(living_area)
        epc_score = self.get_feature(soup, "EPC score (kWh/(m² years))")
        epc_score = int(epc_score) if epc_score else None

        return {
            "property_id" : property_id,
            "price" : price,
            "street": street,
            "postal_code":postal_code,
            "city": city,
            "bedrooms":bedrooms,
            "bathrooms":bathrooms,
            "living_area": living_area,
            "epc_score" : epc_score
        }
    
    def scrape_all(self):
        for url in self.urls:
            try:
                property_data = self.scrape_property(url)
                self.properties.append(property_data)
                print (f"Scraped {url} - Total: {len(self.properties)}")

            except Exception as e:
                print(f"Failed{url}: {e}")   

        
        with open("properties.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.properties[0].keys())
            writer.writeheader()
            writer.writerows(self.properties)


scraper = Scrapping_immoscoop()
scraper.scrape_all()       


        # call scrape_property
        # append result to self.properties
        # print progress
    
    # save to CSV after loop
        #response = self.session.get(url)  
        #soup = BeautifulSoup(response.text, "html.parser")
        
       # bedrooms = self.get_feature(soup, "Number of bedrooms")
       # bathrooms = self.get_feature(soup, "Number of bathrooms")
       # epc = self.get_feature(soup, "EPC score (kWh/(m² years))")
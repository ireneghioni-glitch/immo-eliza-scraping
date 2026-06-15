# CHECKPOINT MANAGING
# Saves the index of the last results page (e.g., "page_index": 45) to a local file (checkpoint.json).
# If the script crashes, it tells the crawler which index to restart from.

# GARANTEE UNIQUENESS
# It must keep track of all the ad URLs already collected.
# A function instantly answers True or False to the question: "Is this URL new or have we already saved it?".

# PERSISTENCE ON DATASET BUILDING
# While the Parser extracts structured data about a house (price, rooms, sqm, etc.),
# this module takes that dictionary and immediately append it to the final dataset file (immovlan_dataset.csv or .jsonl) on real time.

import json
import csv
from pathlib import Path
import threading

from file_Dan import Dan_class
from file_Victor import Victor_class


class StateManager:

    def __init__(self, csv_path: str = "fetched_urls.csv", json_path: str = "checkpoint.json"):
        # creation of Path objs
        self.json_file = Path(json_path)
            # contains a dictionary with keys:
                # last_price_range_index
                # last_page_num
        self.csv_file = Path(csv_path)
            # saves the urls
        # initialization of RAM (ultra-rapid memory) for duplicates
        self.url_set = set()

        # instantiate the lock for threading
        self.file_lock = threading.Lock()

        # initialization of dictionary saving checkpoints: it has
            # as keys: number assigned of price range 
            # as values: number of page last red for that price range
                # for each price range
        self.cordinates_dict = {}
        # use here dictionary stored in Dan_class for extracting price_range_index 
        # for now I'm going to call it price_ranges_dict
        for value in price_ranges_dict.values():
            cordinates_dict[value] = 0

        # check if file already exists on the hard disk
        # If json file already exists, it means that it crushed in the middle of last session
        if self.json_file.exists():
            # open json file
            with open(self.json_file, "r") as coordinates:
                # extraction of last_price_range_index to be passed to main.py
                last_price_range_index = coordinates_dict[last_price_range_index]
                last_page_num = coordinates_dict[last_page_num]
        # check if csv file already exists on the hard disk
        # If csv file already exists, we already have saved urls
        if self.csv_file.exists():
            with open(self.csv_file, "r") as urls:
                for url in urls:
                    self.url_set.add(url)
        else:
            # create csv file from scratch
            with open(self.csv_file, "w") as f:
                url = f.write().split("\n")

    # string argument is the the listing URL or property ID
    # come's from Neha's class
    def is_duplicate(self, string):
        return string in self.save_url

    # dict_data argument is a dictionary containing all the cleaned data extracted by the parser
    # come's from Victor's class
    def save_property_record(self, dict_data):
        # adding properety url to other property infos in property dictionary
        dict_data["property_url"] = string # the one from Neha's
        self.save_url.add(string)

    def save_page_checkpoint(self, price_range_index, page_num): 
        with self.file_lock:
            with open("checkpoint.json", "w") as coordinates_file:

                        
                json.dump(coordinates_dict, coordinates_file) # puts the dictionary with coordinates in file
                # coordinates_dict["last_price_range_index"] = last_price_range_index
                # coordinates_dict["last_page_num"] = last_page_num
            with open("checkpoint.json", "r") as coordinates:
                return coordinates.
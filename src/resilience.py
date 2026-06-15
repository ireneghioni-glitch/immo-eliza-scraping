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

    def __init__(self, provinces: list, csv_path: str = "fetched_urls.csv", json_path: str = "checkpoint.json"):
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

        # check if file already exists on the hard disk
        # If json file already exists, it means that it crushed in the middle of last session
        if self.json_file.exists():
            # open json file
            with open(self.json_file, "r") as coordinates:
                self.coordinates_dict = json.load(coordinates)
                # dictionary has
                    # as keys: province 
                    # as values: number of page last red for that price range
                # for each province
                # use here list provinces that must be injected from main.py
                # each thread on a different province
        else:
            # create json file from scratch
            with open(self.json_file, "w") as coordinates:
                self.coordinates_dict = {}
                for province in provinces:
                    self.coordinates_dict[province] = 0
                json.dump(self.coordinates_dict, coordinates)

        # check if csv file already exists on the hard disk
        # If csv file already exists, we already have saved urls
        if self.csv_file.exists():
            with open(self.csv_file, "r") as urls:
                for url in urls:
                    self.url_set.add(url.strip())
        else:
            # create csv file from scratch
            self.csv_file.touch() # generates the empty csv file

    # string argument is the the listing URL or property ID
    def is_duplicate(self, url):
        return url in self.url_set

    # dict_data argument is a dictionary containing all the cleaned data extracted by the parser
    # come's from Victor's class
    def save_property_record(self, dict_data, url):
        # adding property url to other property infos in property dictionary
        dict_data["property_url"] = url
        self.save_url.add(url)

    def save_page_checkpoint(self, province, last_page_num): 
        with self.file_lock:
            self.coordinates_dict[province] = last_page_num
            with open("checkpoint.json", "w") as coordinates_file:
                json.dump(self.coordinates_dict, coordinates_file) # puts the dictionary with coordinates in file

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
from pathlib import Path
import threading


class StateManager:

    def __init__(self, 
                 csv_path: str = "fetched_urls.csv", 
                 json_path: str = "checkpoint.json",
                 dataset_path: str = "properties.jsonl"):
        # creation of Path objs
        self.json_file = Path(json_path)
            # saves the exact last index in urls_list visited
        self.csv_file = Path(csv_path)
            # saves all urls
        # initialization of JSON Lines text file for properties dataset
        self.jsonl_properties_dataset = Path(dataset_path)

        # no need for this because Dan is already doing it

        #     # saves the urls
        # # initialization of RAM (ultra-rapid memory) for duplicates
        # self.url_set = set()

        # instantiate the lock for threading
        self.file_lock = threading.Lock()

        # check if file already exists on the hard disk
        # If json file already exists, it means that it crushed in the middle of last session
        if self.json_file.exists():
            # open json file
            with open(self.json_file, "r") as url_bookmark:
                self.url_bookmark = json.load(url_bookmark)
                # dictionary has
                    # as keys: province 
                    # as values: number of page last red for that price range
                # for each province
                # --> use here list provinces that must be injected from main.py <--
                # each thread on a different province
        else:
            # create json file from scratch
            with open(self.json_file, "w") as url_bookmark:
                self.url_bookmark = 0
                json.dump(self.url_bookmark, url_bookmark)

        # make sure that the csv file exists on the hard disk
        self.csv_file.touch(exist_ok=True)
    
    # rmed by multiple threads
    # def cothis is not needed because Dan is already doing this in his module, no need for an addictional method for that

    # # Check-Then-Act logic to avoid Race Condition
    # # unique method protected by Lock includes the actions that will be 
    # # perfontrol_and_save(self,url):
    #     with self.file_lock:
    # # former def is_duplicate(self, url) but optimized for this configuration
    #         if url in self.url_set:
    #             return False
    #         self.url_set.add(url)
    #         return True
        
    def save_property_record(self, data, url):
        with self.file_lock:
            # dict_data argument is a dictionary containing all the cleaned data extracted by the parser
            # come's from Victor's class (optimized by Dan)
            # adding property url to other property infos in property dictionary
            data["property_url"] = url
            # i save the dict in a jsonl file
            with open(self.jsonl_properties_dataset, "a") as properties_dataset:
                property_info = json.dumps(data)
                    # grabs the dict and converts it into a string in RAM
                # .write() method only accept a string as argument
                properties_dataset.write(property_info + "\n")
            with open(self.csv_file, "a") as urls_log:
                urls_log.write(url + "\n")

    def save_url_checkpoint(self, saved_index):
        with self.file_lock:
            self.url_bookmark = saved_index # --> will be linked to urls_data in main.py
            # introduce atomic writing to secure the file content
            # creation and writing on temporary file
            temp_json_file = self.json_file.with_name(self.json_file.name + ".tmp")
            with open(temp_json_file, "w") as temp_bookmark:
                json.dump(saved_index, temp_bookmark)
                # puts the dictionary with coordinates in temporary file
            # temporary file becomes the actual file
            temp_json_file.replace(self.json_file)
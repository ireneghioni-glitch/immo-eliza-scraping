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
        self.csv_file = Path(csv_path)
        self.jsonl_properties_dataset = Path(dataset_path)

        # instantiate the lock for threading
        self.file_lock = threading.Lock()

        # check if file already exists on the hard disk
        # If json file already exists, it means that it crashed in the middle of last session
        if self.json_file.exists():
            with open(self.json_file, "r") as url_bookmark:
                completed_list = json.load(url_bookmark)
                self.completed_indices = set(completed_list)  # set of all completed indices
        else:
            # create json file from scratch
            self.completed_indices = set()
            with open(self.json_file, "w") as url_bookmark:
                json.dump(list(self.completed_indices), url_bookmark)

        # make sure that the csv file exists on the hard disk
        self.csv_file.touch(exist_ok=True)

    def save_property_record(self, data, url):
        with self.file_lock:
            # dict_data argument is a dictionary containing all the cleaned data extracted by the parser
            data["property_url"] = url
            # save the dict in a jsonl file
            with open(self.jsonl_properties_dataset, "a") as properties_dataset:
                property_info = json.dumps(data)
                properties_dataset.write(property_info + "\n")
            with open(self.csv_file, "a") as urls_log:
                urls_log.write(url + "\n")

    def save_url_checkpoint(self, saved_index):
        with self.file_lock:
            self.completed_indices.add(saved_index)  # add this index to the set of completed ones

            # atomic writing to secure the file content
            temp_json_file = self.json_file.with_name(self.json_file.name + ".tmp")
            with open(temp_json_file, "w") as temp_bookmark:
                json.dump(list(self.completed_indices), temp_bookmark)
            temp_json_file.replace(self.json_file)

    def is_done(self, index):
        return index in self.completed_indices

    def filter_remaining(self, urls):
        remaining = [(i, url) for i, url in enumerate(urls) if not self.is_done(i)]
        skipped = len(urls) - len(remaining)
        print(f"Resuming — {len(remaining)} remaining, {skipped} already done")
        return remaining
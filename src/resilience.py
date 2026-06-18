"""
resilience.py
=============

Thread-safe state persistence and fault-tolerance orchestrator.

This module provides the core architecture required to guarantee data integrity
and pipeline resilience during concurrent scraping operations. By centralizing I/O
subsystems and tracking operational progress through state checkpoints, it allows the
entire scraping pipeline to handle crashes gracefully and eliminate redundant work.

Core Features
-------------
* ``StateManager``      — Coordinates file handles, memory registries, and synchronization.
* ``completed_indices``  — Memory-cached ``set`` providing O(1) lookups for skipped elements.
* ``Atomic Checkpoints`` — Prevents corruption of tracking files via file-system atomic swaps.

Concurrency model
-----------------
File-system mutation is explicitly I/O-bound and vulnerable to race conditions. 
This module wraps all disk writes inside an internal ``threading.Lock()`` context.
While worker threads fetch and parse assets concurrently, downstream state updates
(appending to JSONL, tracking URLs in CSV, or mutating the JSON checkpoint) are 
strictly serialized to prevent interleaved or partial writes.

Fault Tolerance & Integrity
---------------------------
* **Crash-Resume Pipeline:** The system scans for a pre-existing checkpoint on startup. 
    If found, it populates the index registry, enabling ``filter_remaining`` 
    to seamlessly bypass already-processed items.
* **Atomic Write Protocol:** To prevent corrupt state files if the pipeline or machine drops power mid-write, 
    ``save_url_checkpoint`` dumps updates to a staging file (``.tmp``) and uses an atomic ``Path.replace()`` 
    to overwrite the live production checkpoint.
"""


import json
from pathlib import Path
import threading


class StateManager:

    def __init__(self, 
                 csv_path: Path | str, 
                 json_path: Path | str,
                 dataset_path: Path | str):
        # creation of Path objs
        self.json_file = Path(json_path)
        self.csv_file = Path(csv_path)
        self.jsonl_properties_dataset = Path(dataset_path)

        # instantiate the lock for threading
        self.file_lock = threading.Lock()

        # check if file already exists on the hard disk
        # If json file already exists, it means that it crashed in the middle of last session
        if self.json_file.exists():
            with self.json_file.open("r") as url_bookmark:
            # with open(self.json_file, "r") as url_bookmark:
                completed_list = json.load(url_bookmark)
                self.completed_indices = set(completed_list)  # set of all completed indices
        else:
            # create json file from scratch
            self.completed_indices = set()
            with self.json_file.open("w") as url_bookmark:
            # with open(self.json_file, "w") as url_bookmark:
                json.dump(list(self.completed_indices), url_bookmark)

        # make sure that the csv file exists on the hard disk
        self.csv_file.touch(exist_ok=True)

    def save_property_record(self, data, url):
        with self.file_lock:
            # dict_data argument is a dictionary containing all the cleaned data extracted by the parser
            data["property_url"] = url
            # save the dict in a jsonl file
            with self.jsonl_properties_dataset.open("a") as properties_dataset:
            # with open(self.jsonl_properties_dataset, "a") as properties_dataset:
                property_info = json.dumps(data)
                properties_dataset.write(property_info + "\n")
            with self.csv_file.open("a") as urls_log:
            # with open(self.csv_file, "a") as urls_log:
                urls_log.write(url + "\n")

    def save_url_checkpoint(self, saved_index):
        with self.file_lock:
            self.completed_indices.add(saved_index)  # add this index to the set of completed ones

            # atomic writing to secure the file content
            temp_json_file = self.json_file.with_name(self.json_file.name + ".tmp")
            with temp_json_file.open("w") as temp_bookmark:
            # with open(temp_json_file, "w") as temp_bookmark:
                json.dump(list(self.completed_indices), temp_bookmark)
            temp_json_file.replace(self.json_file)

    def is_done(self, index):
        return index in self.completed_indices

    def filter_remaining(self, urls):
        remaining = [(i, url) for i, url in enumerate(urls) if not self.is_done(i)]
        skipped = len(urls) - len(remaining)
        print(f"Resuming — {len(remaining)} remaining, {skipped} already done")
        return remaining
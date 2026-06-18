"""
main.py
=======

Pipeline entry point. Runs two independent scraping pipelines in sequence, each
producing its own dataset, then deduplicates the Immovlan dataset:

1. Immovlan  — ``run_scraper`` (URL discovery) → ``PropertyScraper`` (parsing)
               → ``PropertyParser.clean_duplicate`` (dedup)
2. ImmoScoop — ``UrlFetch`` (URL discovery)    → ``ImmoScoopScraper`` (parsing)

Each pipeline gets its own ``StateManager`` pointed at a distinct set of files,
so their checkpoints and datasets never overwrite one another. The pipelines run
back-to-back rather than concurrently: each already saturates many threads
internally, so overlapping them would mostly multiply request load (and ban
risk) for no real wall-clock win.
"""

from pathlib import Path

from src.resilience import StateManager               # ← shared state manager
from src.scrapping_thread import run_scraper          # ← Immovlan URL discovery
from src.parsing_thread import PropertyScraper, PropertyParser  # ← Immovlan parser + dedup
from src.url_fetch_immoscoop import UrlFetch          # ← ImmoScoop URL discovery
from src.scrapper_immoscoop import ImmoScoopScraper   # ← ImmoScoop parser/scraper
from codecarbon import EmissionsTracker
from src.parsing_thread import DeadLetterQueue        # ← Immovlan parser class


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tracker = EmissionsTracker()
    tracker.start()
    # Resolve all output paths relative to this file, independent of the current
    # working directory.
    BASE_DIR = Path(__file__).resolve().parent

    # other repo direcories
    DATA_DIR = BASE_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    CLEANED_DIR = DATA_DIR / "cleaned"
    INTERNAL_DIR = DATA_DIR / "internal"

    for folder in [RAW_DIR, CLEANED_DIR, INTERNAL_DIR]:
        folder.mkdir(parents=True, exist_ok=True)

    FAILED_URLS_PATH = INTERNAL_DIR / "failed_urls.txt"
    SCRAPING_ERRORS = INTERNAL_DIR / "scraping_errors.log"

    # ── Pipeline 1 : Immovlan ─────────────────────────────────────────────────
    immovlan_state = StateManager(
        csv_path=INTERNAL_DIR / "fetched_urls.csv",
        json_path=INTERNAL_DIR / "checkpoint.json",
        dataset_path=RAW_DIR / "properties.jsonl",
    )


    immovlan_urls = run_scraper(50)

    immovlan_scraper = PropertyScraper(
        state_manager=immovlan_state, 
        max_concurrent=50,
        failed_url_path=FAILED_URLS_PATH
    
    )
    immovlan_results = immovlan_scraper.run(list(immovlan_urls))
    print(f"Immovlan — {len(immovlan_results)} properties scraped")

    # Deduplicate the Immovlan dataset. Records are persisted as JSONL by the
    # StateManager, so clean_duplicate reads properties.jsonl and writes the
    # cleaned CSV to ./data/cleaned/properties_cleaned.csv.
    PropertyParser.clean_duplicate(RAW_DIR / "properties.jsonl")

    # ── Pipeline 2 : ImmoScoop ────────────────────────────────────────────────
    # Separate StateManager → separate checkpoint + dataset files, so this run
    # produces an independent second dataset (immoscoop_properties.jsonl).
    immoscoop_state = StateManager(
        csv_path=INTERNAL_DIR / "immoscoop_fetched_urls.csv",
        json_path=INTERNAL_DIR / "immoscoop_checkpoint.json",
        dataset_path=RAW_DIR / "immoscoop_properties.jsonl",
    )

    immoscoop_urls = UrlFetch(
        max_pages=5, 
        target=200
    ).fetch_urls()

    immoscoop_scraper = ImmoScoopScraper(
        state_manager=immoscoop_state, 
        max_concurrent=30,
        failed_url_path=FAILED_URLS_PATH
    )
    immoscoop_results = immoscoop_scraper.run(immoscoop_urls)
    print(f"ImmoScoop — {len(immoscoop_results)} properties scraped")
    emissions = tracker.stop()
    print(f"Total emissions: {emissions} kg of CO₂")
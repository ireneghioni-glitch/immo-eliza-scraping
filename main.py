"""
main.py
=======

Pipeline entry point. Runs two independent scraping pipelines in sequence, each
producing its own dataset:

1. Immovlan  — ``run_scraper`` (URL discovery) → ``PropertyScraper`` (parsing)
2. ImmoScoop — ``UrlFetch`` (URL discovery)    → ``ImmoScoopScraper`` (parsing)

Each pipeline gets its own ``StateManager`` pointed at a distinct set of files,
so their checkpoints and datasets never overwrite one another. The pipelines run
back-to-back rather than concurrently: each already saturates many threads
internally, so overlapping them would mostly multiply the request load (and
ban risk) for no real wall-clock win.
"""

from pathlib import Path

from src.resilience import StateManager          # ← shared state manager
from src.scrapping_thread import run_scraper     # ← Immovlan URL discovery
from src.parsing_thread import PropertyScraper   # ← Immovlan parser/scraper
from src.url_fetch_immoscoop import UrlFetch      # ← ImmoScoop URL discovery
from src.scrapper_immoscoop import ImmoScoopScraper  # ← ImmoScoop parser/scraper


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # Resolve all output paths relative to this file, independent of the current
    # working directory.
    BASE_DIR = Path(__file__).resolve().parent

    # ── Pipeline 1 : Immovlan ─────────────────────────────────────────────────
    immovlan_state = StateManager(
        csv_path=BASE_DIR / "fetched_urls.csv",
        json_path=BASE_DIR / "checkpoint.json",
        dataset_path=BASE_DIR / "properties.jsonl",
    )

    immovlan_urls = run_scraper(50)

    immovlan_scraper = PropertyScraper(state_manager=immovlan_state, max_concurrent=50)
    immovlan_results = immovlan_scraper.run(list(immovlan_urls))
    print(f"Immovlan — {len(immovlan_results)} properties scraped")

    # ── Pipeline 2 : ImmoScoop ────────────────────────────────────────────────
    # Separate StateManager → separate checkpoint + dataset files, so this run
    # produces an independent second dataset (immoscoop_properties.jsonl).
    immoscoop_state = StateManager(
        csv_path=BASE_DIR / "immoscoop_fetched_urls.csv",
        json_path=BASE_DIR / "immoscoop_checkpoint.json",
        dataset_path=BASE_DIR / "immoscoop_properties.jsonl",
    )

    immoscoop_urls = UrlFetch(max_pages=5, target=200).fetch_urls()

    immoscoop_scraper = ImmoScoopScraper(state_manager=immoscoop_state, max_concurrent=30)
    immoscoop_results = immoscoop_scraper.run(immoscoop_urls)
    print(f"ImmoScoop — {len(immoscoop_results)} properties scraped")
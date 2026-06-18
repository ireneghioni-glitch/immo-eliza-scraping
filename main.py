from pathlib import Path
from src.resilience import StateManager  # ← import the state manager
from src.scrapping_thread import run_scraper # ← import run_scraper
from src.parsing_thread import PropertyScraper # ← import the property scraper


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    
    BASE_DIR = Path(__file__).resolve().parent # ← substitutes os.path.dirname(os.path.abspath(__file__))

    state_manager = StateManager(
        csv_path=BASE_DIR/"fetched_urls.csv",
        json_path=BASE_DIR/"checkpoint.json",
        dataset_path=BASE_DIR/"properties.jsonl"
    )

    urls = run_scraper(50)

    # output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.csv")
    # state_manager = StateManager(
    #     csv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "fetched_urls.csv"),
    #     json_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoint.json"),
    #     dataset_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.jsonl"),
    # )

    scraper = PropertyScraper(state_manager=state_manager, max_concurrent=50) # deleted output_file=output_path
    results = scraper.run(list(url))
import json
import requests
from datetime import datetime
from pathlib import Path
from logger_config import setup_logging

logger = setup_logging(log_file=Path(__file__).parent.parent / "logs" / "pipeline.log")


class DataExtractor:
    """Class for extracting data from REST API"""

    def __init__(self, base_url: str, data_dir: str = "data"):
        self.base_url = base_url
        self.data_dir = Path(__file__).parent.parent / data_dir
        self.raw_dir = self.data_dir / "raw"

    def create_directories(self) -> Path:
        """Create necessary directories based on today's date"""

        today = datetime.now().strftime("%Y-%m-%d")
        raw_today_dir = self.raw_dir / today
        raw_today_dir.mkdir(parents=True, exist_ok=True)

        return raw_today_dir

    def fetch_data(self, endpoint: str, params: dict = None) -> dict:
        """Fetch data from REST API"""

        url = f"{self.base_url.rstrip('/')}{endpoint}"

        try:
            logger.info(f"Fetching data from {url}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            logger.info(f"Successfully fetched data from {url}")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise e

    def save_raw_data(self, data: dict, filename: str = "response.json") -> Path:
        """Save raw data to file"""

        raw_dir = self.create_directories()
        file_path = raw_dir / filename

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved raw data to {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to save raw data to {file_path}: {e}")
            raise e

    def extract_and_save(
        self, endpoint: str, filename: str, params: dict = None
    ) -> dict:
        """Full cycle: fetch and save data"""

        data = self.fetch_data(endpoint, params)
        self.save_raw_data(data, filename)

        logger.info(f"Extracted data to {filename} from {endpoint}")
        return data


def main():
    """Example usage of the module"""
    extractor = DataExtractor(base_url="https://jsonplaceholder.typicode.com")

    users_data = extractor.extract_and_save("/users", filename="users.json")
    posts_data = extractor.extract_and_save("/posts", filename="posts.json")


if __name__ == "__main__":
    main()

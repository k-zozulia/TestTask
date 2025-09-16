import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Class for transforming and cleaning raw API data"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"

    def create_processed_directory(self):
        """Create directory for processed data"""

        today = datetime.now().strftime("%Y-%m-%d")
        processed_today_dir = self.processed_dir / today
        processed_today_dir.mkdir(parents=True, exist_ok=True)
        return processed_today_dir

    def load_raw_data(self, filename: str, date: str = None):
        """Load raw data from file"""

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        file_path = self.raw_dir / date / filename

        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} does not exist")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"Loading raw data from {file_path}")
        return data

    def transform_users_data(self, users_data: list) -> pd.DataFrame:
        """Transform users data"""

        df = pd.DataFrame(users_data)

        df_transformed = pd.DataFrame()

        df_transformed["user_id"] = df["id"]
        df_transformed["username"] = df["username"]
        df_transformed["name"] = df["name"]
        df_transformed["email"] = df["email"].str.lower()
        df_transformed["phone"] = df["phone"]
        df_transformed["website"] = df["website"]
        df_transformed["city"] = df["address"].apply(
            lambda x: x.get("city", "") if isinstance(x, dict) else ""
        )
        df_transformed["zipcode"] = df["address"].apply(
            lambda x: x.get("zipcode", "") if isinstance(x, dict) else ""
        )
        df_transformed["lat"] = pd.to_numeric(
            df["address"].apply(
                lambda x: (
                    x.get("geo", {}).get("lat", None) if isinstance(x, dict) else None
                )
            ),
            errors="coerce",
        )
        df_transformed["lng"] = pd.to_numeric(
            df["address"].apply(
                lambda x: (
                    x.get("geo", {}).get("lng", None) if isinstance(x, dict) else None
                )
            ),
            errors="coerce",
        )
        df_transformed["company_name"] = df["company"].apply(
            lambda x: x.get("name", "") if isinstance(x, dict) else ""
        )
        df_transformed["company_catchphrase"] = df["company"].apply(
            lambda x: x.get("catchPhrase", "") if isinstance(x, dict) else ""
        )
        df_transformed["email_domain"] = df_transformed["email"].str.split("@").str[1]
        df_transformed["has_coordinates"] = ~(
            df_transformed["lat"].isna() | df_transformed["lng"].isna()
        )
        df_transformed["created_at"] = datetime.now().isoformat()

        df_transformed = df_transformed.fillna("")

        logger.info(f"Transforming {len(df_transformed)} users data")
        return df_transformed

    def transform_posts_data(self, posts_data: list) -> pd.DataFrame:
        """Transform posts data"""

        df = pd.DataFrame(posts_data)
        df_transformed = pd.DataFrame()

        df_transformed["post_id"] = df["id"]
        df_transformed["user_id"] = df["userId"]
        df_transformed["title"] = df["title"].str.strip()
        df_transformed["body"] = df["body"].str.strip()

        df_transformed["title_length"] = df_transformed["title"].str.len()
        df_transformed["body_length"] = df_transformed["body"].str.len()
        df_transformed["word_count"] = df_transformed["body"].str.split().str.len()
        df_transformed["created_at"] = datetime.now().isoformat()

        df_transformed["post_category"] = pd.cut(
            df_transformed["body_length"],
            bins=[0, 100, 200, float("inf")],
            labels=["short", "medium", "long"],
        ).astype(str)

        logger.info(f"Transforming {len(df_transformed)} posts data")
        return df_transformed

    def save_to_parquet(self, df: pd.DataFrame, filename: str) -> Path:
        """Save dataframe to parquet file"""

        processed_dir = self.create_processed_directory()
        file_path = processed_dir / filename

        table_data = pa.Table.from_pandas(df)
        pq.write_table(table_data, file_path)

        logger.info(f"Saving {file_path} into parquet file")
        return file_path

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> Path:
        """Save dataframe to csv file"""

        processed_dir = self.create_processed_directory()
        file_path = processed_dir / filename

        df.to_csv(file_path, index=False, encoding="utf-8")
        logger.info(f"Saving {file_path} into csv file")

        return file_path

    def process_data(
        self, raw_filename: str, processed_filename: str, data_type: str = "users"
    ) -> pd.DataFrame:
        """Process raw API data"""

        raw_data = self.load_raw_data(raw_filename)

        if data_type == "users":
            df_processed = self.transform_users_data(raw_data)
        elif data_type == "posts":
            df_processed = self.transform_posts_data(raw_data)
        else:
            raise ValueError("Unknown data type")

        self.save_to_parquet(df_processed, processed_filename)

        return df_processed


def main():
    """Example usage of the module"""

    transformer = DataTransformer()

    try:
        users_df = transformer.process_data("users.json", "users.parquet", "users")
        posts_df = transformer.process_data("posts.json", "posts.parquet", "posts")

    except Exception as error:
        logger.error(error)


if __name__ == "__main__":
    main()

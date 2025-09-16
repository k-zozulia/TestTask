import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Text,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pyarrow.parquet as pq
from logger_config import setup_logging

logger = setup_logging(log_file=Path(__file__).parent.parent / "logs" / "pipeline.log")

Base = declarative_base()

LENGTH_RULES = {
    "users": {
        "username": 100,
        "name": 200,
        "email": 200,
        "phone": 50,
        "website": 200,
        "city": 100,
        "zipcode": 20,
        "company_name": 200,
        "email_domain": 100,
    },
    "posts": {
        "title": 500,
        "post_category": 20,
    },
}


class User(Base):
    """User table model"""

    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True)
    username = Column(String(100), nullable=False)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=False)
    phone = Column(String(50))
    website = Column(String(200))
    city = Column(String(100))
    zipcode = Column(String(20))
    lat = Column(Float)
    lng = Column(Float)
    company_name = Column(String(200))
    company_catchphrase = Column(Text)
    email_domain = Column(String(100))
    has_coordinates = Column(Boolean)
    created_at = Column(DateTime)


class Post(Base):
    """Posts table model"""

    __tablename__ = "posts"

    post_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    title = Column(String(500), nullable=False)
    body = Column(Text, nullable=False)
    title_length = Column(Integer)
    body_length = Column(Integer)
    word_count = Column(Integer)
    post_category = Column(String(20))
    created_at = Column(DateTime)


class DatabaseLoader:
    """Class for managing database loading"""

    def __init__(self, db_path: str = "local.db", data_dir: str = "data"):
        self.db_path = Path(__file__).parent.parent / db_path
        self.data_dir = Path(__file__).parent.parent / data_dir
        self.processed_dir = self.data_dir / "processed"
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables in database"""

        logger.info("Creating tables...")
        Base.metadata.create_all(self.engine)
        logger.info("Tables created successfully")

    def load_processed_data(self, filename: str, date: str = None) -> pd.DataFrame:
        """Load processed data"""

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        file_path = self.processed_dir / date / filename

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        table = pq.read_table(file_path)
        df = table.to_pandas()

        logger.info(f"Loaded {len(df)} records from: {file_path}")
        return df

    def clean_dataframe_for_db(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """
        Prepare DataFrame for database insertion:
        - enforce string length limits according to schema
        - convert created_at to datetime if exists
        """
        df_clean = df.copy()

        if "created_at" in df_clean.columns:
            df_clean["created_at"] = pd.to_datetime(
                df_clean["created_at"], errors="coerce"
            )

        if table_type in LENGTH_RULES:
            for col, max_len in LENGTH_RULES[table_type].items():
                if col in df_clean.columns and pd.api.types.is_string_dtype(
                    df_clean[col]
                ):
                    df_clean[col] = df_clean[col].astype(str).str[:max_len]

        logger.info(f"Cleaned {table_type} dataframe")
        return df_clean

    def load_to_database(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"
    ) -> None:
        """
        Load DataFrame into database table.
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"Inserted {len(df)} rows into table {table_name}")

        except Exception as e:
            logger.error(f"Failed to insert {table_name}: {e}")
            raise e

    def verify_data_load(self, table_name: str) -> dict:
        """
        Verify number of rows in a table.
        """
        query = text(f"SELECT COUNT(*) as count FROM {table_name}")
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            count = result[0] if result else 0

        logger.info(f"Table {table_name} contains {count} rows")
        return {"table": table_name, "count": count}

    def get_table_info(self) -> dict:
        """
        Get info about all tables in database.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)

        table_info = {}
        for table_name in metadata.tables.keys():
            stats = self.verify_data_load(table_name)
            table_info[table_name] = stats

        return table_info

    def load_all_data(self, date: str = None) -> dict:
        """
        Load all processed parquet data into DB.
        """
        results = {}

        self.create_tables()

        try:
            users_df = self.load_processed_data("users.parquet", date)
            users_df_clean = self.clean_dataframe_for_db(users_df, "users")
            self.load_to_database(users_df_clean, "users")
            results["users"] = self.verify_data_load("users")

        except Exception as e:
            logger.warning(f"Users file not found: {e}")
            results["users"] = {"table": "users", "count": 0, "error": str(e)}

        try:
            posts_df = self.load_processed_data("posts.parquet", date)
            posts_df_clean = self.clean_dataframe_for_db(posts_df, "posts")
            self.load_to_database(posts_df_clean, "posts")
            results["posts"] = self.verify_data_load("posts")

        except Exception as e:
            logger.warning(f"Posts file not found: {e}")
            results["posts"] = {"table": "posts", "count": 0, "error": str(e)}

        logger.info(f"All tables loaded")
        return results

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute custom SQL query and return results as DataFrame.
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            logger.info(f"Query executed, got {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error while executing query: {e}")
            raise


def main():
    """Example usage of DatabaseLoader"""
    loader = DatabaseLoader()

    try:
        results = loader.load_all_data()
        print("Load results:")
        for table, stats in results.items():
            if "error" in stats:
                print(f"  {table}: ERROR - {stats['error']}")
            else:
                print(f"  {table}: {stats['count']} rows")

        table_info = loader.get_table_info()
        print(f"\nTable info: {table_info}")

        test_query = "SELECT COUNT(*) as total_users FROM users"
        result = loader.execute_query(test_query)
        print(f"\nTest query: {result}")

    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()

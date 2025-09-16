import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class DataAnalytics:
    """Class for running analytics queries and generating reports."""

    def __init__(self, db_path: str = "local.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)

    def execute_query(self, query: str, query_name: str = "") -> pd.DataFrame:
        """Execute a query and return a dataframe."""

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, con=conn)

            logger.info(f"Executed query: {query_name}, received {len(df)} rows.")
            return df

        except Exception as e:
            logger.error(f"Failed to execute query: {query_name}, error: {e}")
            raise e

    def user_statistics(self) -> pd.DataFrame:
        """Basic user statistics."""

        query = """
                SELECT COUNT(*)                                        as total_users,
                       COUNT(DISTINCT email_domain)                    as unique_domains,
                       COUNT(CASE WHEN has_coordinates = 1 THEN 1 END) as users_with_coordinates,
                       COUNT(CASE WHEN company_name != '' THEN 1 END)  as users_with_company
                FROM users
                """
        return self.execute_query(query, "user_statistics")

    def post_statistics(self) -> pd.DataFrame:
        """Post statistics."""

        query = """
                SELECT COUNT(*)                as total_posts,
                       COUNT(DISTINCT user_id) as unique_authors,
                       AVG(title_length)       as avg_title_length,
                       AVG(body_length)        as avg_body_length,
                       AVG(word_count)         as avg_word_count,
                       MAX(body_length)        as max_body_length,
                       MIN(body_length)        as min_body_length
                FROM posts
                """
        return self.execute_query(query, "post_statistics")

    def user_post_activity(self) -> pd.DataFrame:
        """User post activity."""

        query = """
                SELECT u.user_id,
                       u.username,
                       u.name,
                       u.email_domain,
                       COUNT(p.post_id)   as post_count,
                       AVG(p.word_count)  as avg_post_length,
                       MAX(p.body_length) as max_post_length
                FROM users u
                         LEFT JOIN posts p ON u.user_id = p.user_id
                GROUP BY u.user_id, u.username, u.name, u.email_domain
                ORDER BY post_count DESC
                """
        return self.execute_query(query, "user_post_activity")

    def generate_report(self) -> dict:
        """Generate report with three main queries."""

        logger.info("Generating report.")

        report = {
            "generated_at": datetime.now().isoformat(),
            "database": self.db_path,
            "analytics": {}
        }

        analytics_functions = [
            ("user_statistics", self.user_statistics),
            ("post_statistics", self.post_statistics),
            ("user_post_activity", self.user_post_activity)
        ]

        for name, func in analytics_functions:
            try:
                df = func()
                report["analytics"][name] = {
                    "data": df.to_dict('records'),
                    "record_count": len(df),
                    "columns": df.columns.tolist()
                }

            except Exception as e:
                logger.error(f"Error in {name}: {e}")
                report["analytics"][name] = {
                    "error": str(e),
                    "data": [],
                    "record_count": 0
                }

        return report

    def save_csv_reports(self, report: dict) -> list:
        """Save each query result to separate CSV file."""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_files = []

        for analysis_name, analysis_data in report["analytics"].items():
            if analysis_data.get("data") and len(analysis_data["data"]) > 0:
                df = pd.DataFrame(analysis_data["data"])
                filename = f"{analysis_name}_{timestamp}.csv"
                file_path = self.reports_dir / filename

                df.to_csv(file_path, index=False, encoding='utf-8')
                saved_files.append(file_path)
                logger.info(f"CSV saved: {file_path}")

        return saved_files

    def save_json_report(self, report: dict) -> Path:
        """Save summary JSON report."""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"summary_report_{timestamp}.json"
        file_path = self.reports_dir / filename

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"JSON report saved: {file_path}")
        return file_path

    def run_analytics(self) -> dict:
        """Run analytics and save reports."""

        logger.info("Starting analytics run.")

        report = self.generate_report()

        csv_files = self.save_csv_reports(report)

        json_file = self.save_json_report(report)

        result = {
            "json_report": str(json_file),
            "csv_files": [str(f) for f in csv_files],
            "queries_executed": list(report["analytics"].keys()),
            "total_csv_files": len(csv_files)
        }

        logger.info(f"Analytics completed. Generated {len(csv_files)} CSV files and 1 JSON report.")
        return result


def main():
    """Example usage of DataAnalytics"""

    analytics = DataAnalytics()

    try:
        results = analytics.run_analytics()
        print(f"Reports generated:")
        print(f"JSON report: {results['json_report']}")
        print(f"CSV files: {results['csv_files']}")

    except Exception as e:
        logger.error(f"Error running analytics: {e}")


if __name__ == "__main__":
    main()
import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent / "src"))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DatabaseLoader
from src.analytics import DataAnalytics
from src.logger_config import setup_logging

logger = setup_logging(log_file=Path(__file__).parent / "logs" / "pipeline.log")


class DataPipeline:
    """Main class for managing data pipeline"""

    def __init__(
        self,
        api_url: str = "https://jsonplaceholder.typicode.com",
        db_path: str = "local.db",
        data_dir: str = "data",
    ):
        self.api_url = api_url
        self.db_path = db_path
        self.data_dir = data_dir

        self.extractor = DataExtractor(api_url, data_dir)
        self.transformer = DataTransformer(data_dir)
        self.loader = DatabaseLoader(db_path, data_dir)
        self.analytics = DataAnalytics(db_path)

        logger.info(f"Pipeline initialized: API={api_url}, DB={db_path}")

    def run_extract(self) -> dict:
        """Extract data stage"""

        logger.info("=== STAGE 1: DATA EXTRACTION ===")

        results = {}

        try:
            users_data = self.extractor.extract_and_save(
                "/users", filename="users.json"
            )
            results["users"] = {
                "success": True,
                "count": len(users_data),
                "data": users_data,
            }

            posts_data = self.extractor.extract_and_save(
                "/posts", filename="posts.json"
            )
            results["posts"] = {
                "success": True,
                "count": len(posts_data),
                "data": posts_data,
            }

            logger.info(
                f"Extraction completed: {len(users_data)} users, {len(posts_data)} posts"
            )

        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            results["error"] = str(e)
            raise

        return results

    def run_transform(self) -> dict:
        """Transform data stage"""

        logger.info("=== STAGE 2: DATA TRANSFORMATION ===")

        results = {}

        try:
            users_df = self.transformer.process_data(
                "users.json", "users.parquet", "users"
            )
            results["users"] = {
                "success": True,
                "count": len(users_df),
                "columns": users_df.columns.tolist(),
            }

            posts_df = self.transformer.process_data(
                "posts.json", "posts.parquet", "posts"
            )
            results["posts"] = {
                "success": True,
                "count": len(posts_df),
                "columns": posts_df.columns.tolist(),
            }

            logger.info(
                f"Transformation completed: {len(users_df)} users, {len(posts_df)} posts"
            )

        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            results["error"] = str(e)
            raise

        return results

    def run_load(self) -> dict:
        """Load data stage"""

        logger.info("=== STAGE 3: LOAD TO DATABASE ===")

        try:
            results = self.loader.load_all_data()

            total_records = sum(
                r.get("count", 0) for r in results.values() if "error" not in r
            )
            logger.info(f"Loading completed: {total_records} records in total")

            return results

        except Exception as e:
            logger.error(f"Error during database loading: {e}")
            raise

    def run_analytics(self) -> dict:
        """Analytics and report generation stage"""

        logger.info("=== STAGE 4: ANALYTICS AND REPORTS ===")

        try:
            results = self.analytics.run_analytics()

            logger.info("Analytics completed:")
            logger.info(f"  - JSON report: {results['json_report']}")
            logger.info(f"  - CSV files: {results['total_csv_files']} files generated")
            logger.info(
                f"  - Queries executed: {', '.join(results['queries_executed'])}"
            )

            return results

        except Exception as e:
            logger.error(f"Error during analytics: {e}")
            raise

    def run_full_pipeline(self) -> dict:
        """Run the entire pipeline"""

        logger.info("STARTING FULL DATA PIPELINE")
        start_time = datetime.now()

        pipeline_results = {"start_time": start_time.isoformat(), "stages": {}}

        try:
            # Stage 1: Extract
            pipeline_results["stages"]["extract"] = self.run_extract()

            # Stage 2: Transform
            pipeline_results["stages"]["transform"] = self.run_transform()

            # Stage 3: Load
            pipeline_results["stages"]["load"] = self.run_load()

            # Stage 4: Analytics
            pipeline_results["stages"]["analytics"] = self.run_analytics()

            # Summary
            end_time = datetime.now()
            duration = end_time - start_time

            pipeline_results["end_time"] = end_time.isoformat()
            pipeline_results["duration_seconds"] = duration.total_seconds()
            pipeline_results["success"] = True

            logger.info(
                f"PIPELINE SUCCESSFULLY COMPLETED in {duration.total_seconds():.2f} seconds"
            )

        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time

            pipeline_results["end_time"] = end_time.isoformat()
            pipeline_results["duration_seconds"] = duration.total_seconds()
            pipeline_results["success"] = False
            pipeline_results["error"] = str(e)

            logger.error(f"PIPELINE FAILED: {e}")
            raise

        return pipeline_results

    def run_single_stage(self, stage: str) -> dict:
        """Run a single pipeline stage"""
        stages = {
            "extract": self.run_extract,
            "transform": self.run_transform,
            "load": self.run_load,
            "analytics": self.run_analytics,
        }

        if stage not in stages:
            raise ValueError(
                f"Unknown stage: {stage}. Available: {list(stages.keys())}"
            )

        logger.info(f"Running stage: {stage}")
        return stages[stage]()


def create_project_structure():
    """Create the base project structure"""
    directories = ["src", "data/raw", "data/processed", "reports"]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)

    (Path("src") / "__init__.py").touch()

    logger.info("Project structure created")


def main():
    """Main function with command-line arguments"""

    parser = argparse.ArgumentParser(description="Data Pipeline")
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "load", "analytics", "full"],
        default="full",
        help="Stage to run",
    )
    parser.add_argument(
        "--api-url",
        default="https://jsonplaceholder.typicode.com",
        help="API URL for data extraction",
    )
    parser.add_argument("--db-path", default="local.db", help="Path to database file")
    parser.add_argument("--data-dir", default="data", help="Directory for storing data")
    parser.add_argument("--setup", action="store_true", help="Create project structure")

    args = parser.parse_args()

    if args.setup:
        create_project_structure()
        return

    pipeline = DataPipeline(
        api_url=args.api_url, db_path=args.db_path, data_dir=args.data_dir
    )

    try:
        if args.stage == "full":
            results = pipeline.run_full_pipeline()
        else:
            results = pipeline.run_single_stage(args.stage)

        print("\n" + "=" * 50)
        print("PIPELINE EXECUTION RESULTS:")
        print("=" * 50)

        if args.stage == "full" and results.get("success"):
            print(f"Duration: {results['duration_seconds']:.2f} seconds")

            for stage, data in results["stages"].items():
                if stage == "analytics":
                    print(f"\n{stage.upper()}:")
                    if "summary" in data:
                        for key, value in data["summary"].items():
                            print(f"  {key}: {value}")
                else:
                    if "error" not in data:
                        print(f"\n{stage.upper()}: success")
                        for key, value in data.items():
                            if isinstance(value, dict) and "count" in value:
                                print(f"  {key}: {value['count']} records")
        else:
            print(f"Stage {args.stage}: executed successfully")

    except Exception as e:
        print(f"Execution error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

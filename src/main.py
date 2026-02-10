"""
main.py

Entry point of the Sales ETL pipeline.

This script orchestrates the full ETL flow:
1) Load config and secrets
2) Create Spark session
3) Extract raw data from CSV
4) Transform data (DQ + schema mapping)
5) Load into Postgres with incremental logic
6) Log all steps for observability

This file is intentionally kept small and readable.
All business logic is delegated to modules in src/.
"""

import os
from pyspark.sql import SparkSession

from src.config import load_config
from src.logger import setup_logger
from src.extract import extract_sales_csv
from src.transform import transform_sales
from src.load import load_sales_incremental


def get_spark(cfg):
    """
    Creates and returns a SparkSession using settings from config.yaml.

    Why is Spark session creation isolated in a function?
    - Improves readability of main()
    - Makes it easier to unit test (or mock) Spark session creation
    - Centralizes Spark configuration

    Parameters
    ----------
    cfg : dict
        Application config dictionary.

    Returns
    -------
    pyspark.sql.SparkSession
    """

    spark = (
        SparkSession.builder
        .master(cfg["spark"]["master"])
        .appName(cfg["spark"]["app_name"])
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .getOrCreate()
    )

    return spark


def main():
    """
    Orchestrates the ETL process end-to-end.

    Notes:
    - We use try/except/finally to ensure Spark is always stopped.
    - We use structured logging to make debugging and monitoring easier.
    """

    # Initialize application logger (file + console).
    logger = setup_logger()

    # Spark session will be created after config is loaded.
    spark = None

    try:
        logger.info("Starting Sales ETL...")

        # ---------------------------------------------------------------------
        # 1) Load configuration and secrets
        # ---------------------------------------------------------------------
        cfg = load_config()
        logger.info("Config loaded successfully")

        # ---------------------------------------------------------------------
        # 2) Create Spark session
        # ---------------------------------------------------------------------
        spark = get_spark(cfg)
        logger.info("Spark session created")

        # ---------------------------------------------------------------------
        # 3) Resolve paths from config
        # ---------------------------------------------------------------------
        raw_path = os.path.join(cfg["paths"]["raw_data_dir"], "sales.csv")

        # Watermark file stores the last successfully loaded transaction_id.
        # This enables incremental loading.
        state_path = os.path.join(cfg["paths"]["state_dir"], "watermark.json")

        # ---------------------------------------------------------------------
        # 4) Extract
        # ---------------------------------------------------------------------
        logger.info(f"Extracting data from: {raw_path}")
        raw_df = extract_sales_csv(spark, raw_path)

        # ---------------------------------------------------------------------
        # 5) Transform
        # ---------------------------------------------------------------------
        logger.info("Transforming data (DQ + schema mapping)")
        transformed_df = transform_sales(raw_df, cfg)

        # ---------------------------------------------------------------------
        # 6) Load (incremental)
        # ---------------------------------------------------------------------
        logger.info("Loading data into Postgres (incremental)")
        load_sales_incremental(transformed_df, cfg, state_path, logger=logger)

        logger.info("ETL finished successfully")

    except Exception as e:
        # logger.exception prints the full stack trace.
        # This is very important for troubleshooting production incidents.
        logger.exception(f"ETL failed with error: {str(e)}")
        raise

    finally:
        # Ensure Spark stops even if ETL fails.
        if spark:
            spark.stop()
            logger.info("Spark stopped")


# Standard Python entrypoint pattern.
if __name__ == "__main__":
    main()

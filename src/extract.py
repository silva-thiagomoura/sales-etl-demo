"""
extract.py

This module contains the extraction logic of the ETL pipeline.

Responsibilities:
- Read the raw sales CSV file using Spark.
- Ensure only expected columns are selected.
- Be resilient to extra columns in the source file (schema evolution).

Why Spark for extraction?
- Spark can read large CSV files efficiently in a distributed way.
- It allows scaling to millions of records with minimal code changes.
"""

from pyspark.sql import functions as F


def extract_sales_csv(spark, path: str):
    """
    Reads sales transactions from a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Active SparkSession used to read data.
    path : str
        Path to the CSV file.

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame containing only the expected columns:
        - transaction_id
        - customer_id
        - product_id
        - quantity
        - timestamp

    Notes
    -----
    - We enable header=True because the CSV contains column names.
    - We use inferSchema=True for simplicity in this exercise.
      In production, defining an explicit schema is recommended for stability.
    - We select only required columns to ignore extra fields if the CSV evolves.
    """

    # Read raw CSV from disk.
    df = (
        spark.read
        .option("header", True)       # CSV includes column names in the first row
        .option("inferSchema", True)  # Spark infers data types automatically
        .csv(path)
    )

    # Select only the required columns.
    # This makes the pipeline robust to extra columns added in the future.
    df = df.select(
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("timestamp")
    )

    return df


"""
transform.py

This module contains the transformation layer of the ETL pipeline.

Responsibilities:
- Handle schema evolution (missing columns).
- Cast raw string fields into the correct data types.
- Apply data quality checks (nulls, outliers, inconsistencies).
- Map the raw schema into the destination schema required by Postgres.

This is the most important layer of the pipeline because it guarantees
data correctness before writing into the database.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def transform_sales(df, cfg):
    """
    Transforms the raw sales DataFrame into the final schema expected by the
    destination database.

    Steps:
    1) Schema evolution support:
       - If required columns are missing, create them with null values.
    2) Type casting:
       - Cast all columns into expected types.
    3) Data quality checks:
       - Remove rows with missing critical fields.
       - Remove rows with invalid quantity values (outliers).
    4) Schema mapping:
       - Convert timestamp -> sale_date.
       - Select final destination columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Raw DataFrame extracted from the CSV file.
    cfg : dict
        Application config dictionary (reserved for future improvements, e.g.
        configurable thresholds, DQ rules, etc.).

    Returns
    -------
    pyspark.sql.DataFrame
        Clean and standardized DataFrame ready to be loaded into Postgres.
    """

    # -------------------------------------------------------------------------
    # 1) Schema evolution support
    # -------------------------------------------------------------------------
    # If the input CSV changes and a column is missing, the pipeline should
    # still run without crashing.
    #
    # This makes the ETL more resilient and backward-compatible.
    #
    # NOTE:
    # In a production-grade system, we would likely enforce strict schemas and
    # fail fast if critical columns are missing. For this exercise, we choose
    # to be permissive and resilient.
    required_cols = ["transaction_id", "customer_id", "product_id", "quantity", "timestamp"]

    for c in required_cols:
        if c not in df.columns:
            # Create missing column as NULL.
            # We cast to string first because raw CSV ingestion often results
            # in strings and this prevents casting errors later.
            df = df.withColumn(c, F.lit(None).cast(StringType()))

    # -------------------------------------------------------------------------
    # 2) Type casting
    # -------------------------------------------------------------------------
    # We explicitly cast columns to enforce consistency and avoid "dirty"
    # values being loaded into the database.
    #
    # - transaction_id must be integer (PK candidate).
    # - customer_id/product_id are numeric identifiers.
    # - quantity is integer.
    # - timestamp is converted into a Spark TimestampType.
    df = (
        df.withColumn("transaction_id", F.col("transaction_id").cast(IntegerType()))
          .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
          .withColumn("product_id", F.col("product_id").cast(IntegerType()))
          .withColumn("quantity", F.col("quantity").cast(IntegerType()))
          .withColumn("timestamp", F.to_timestamp("timestamp"))
    )

    # -------------------------------------------------------------------------
    # 3) Data Quality Checks (DQ)
    # -------------------------------------------------------------------------
    # Missing values:
    # These fields are mandatory for the destination schema.
    # If any of them are missing, the row is not useful and should be dropped.
    df = df.filter(F.col("transaction_id").isNotNull())
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.filter(F.col("product_id").isNotNull())
    df = df.filter(F.col("quantity").isNotNull())
    df = df.filter(F.col("timestamp").isNotNull())

    # Outlier checks:
    # Quantity should be within a reasonable range for a sales transaction.
    # This prevents invalid values (e.g. -999, 1000000) from polluting analytics.
    #
    # NOTE:
    # Thresholds could be moved to config.yaml for better flexibility.
    df = df.filter((F.col("quantity") > 0) & (F.col("quantity") <= 100))

    # -------------------------------------------------------------------------
    # 4) Schema mapping (source -> destination)
    # -------------------------------------------------------------------------
    # The destination table expects a column called "sale_date".
    # We derive it from the timestamp by extracting only the date part.
    df = df.withColumn("sale_date", F.to_date("timestamp"))

    # Final destination schema:
    # We select only the required columns and enforce column ordering.
    df = df.select(
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "sale_date"
    )

    return df

"""
load.py

This module contains the loading layer of the ETL pipeline.

Responsibilities:
- Implement incremental loading using a watermark file.
- Secure sensitive fields before storage (hashing customer_id/product_id).
- Write data into Postgres using Spark JDBC.
- Update the watermark after successful load.

Important notes:
- Spark JDBC write does not support Postgres "ON CONFLICT DO NOTHING".
  For this reason, the pipeline relies on incremental filtering via watermark.
- In a production system, a staging table + MERGE/UPSERT strategy would be used.
"""

import os
import json
from pyspark.sql import functions as F

from src.encryption import encrypt_ids


def read_watermark(path):
    """
    Reads the watermark (last loaded transaction_id) from a JSON file.

    Parameters
    ----------
    path : str
        Path to watermark file.

    Returns
    -------
    int or None
        Last loaded transaction_id, or None if file does not exist.
    """

    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        return json.load(f).get("last_transaction_id")


def write_watermark(path, last_id):
    """
    Writes the watermark (last loaded transaction_id) into a JSON file.

    Parameters
    ----------
    path : str
        Path to watermark file.
    last_id : int
        The latest successfully loaded transaction_id.
    """

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        json.dump({"last_transaction_id": int(last_id)}, f)


def load_sales_incremental(df, cfg, state_path, logger=None):
    """
    Loads sales data into Postgres using incremental logic.

    Incremental strategy:
    - Read the watermark (last loaded transaction_id).
    - Filter the DataFrame to keep only records with transaction_id > watermark.
    - Write new records to Postgres.
    - Update watermark to the maximum transaction_id loaded.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Transformed DataFrame (already cleaned and mapped).
    cfg : dict
        Application configuration dictionary.
    state_path : str
        Path to the watermark JSON file.
    logger : logging.Logger or None
        Optional logger for observability.
    """

    # -------------------------------------------------------------------------
    # 1) Read watermark (incremental checkpoint)
    # -------------------------------------------------------------------------
    last_id = read_watermark(state_path)

    if last_id:
        if logger:
            logger.info(f"Incremental mode enabled. last_transaction_id={last_id}")

        # Keep only new transactions since last successful run.
        df = df.filter(F.col("transaction_id") > F.lit(last_id))

    else:
        if logger:
            logger.info("No watermark found. Running full load (first run).")

    # -------------------------------------------------------------------------
    # 2) Stop early if there is nothing new to load
    # -------------------------------------------------------------------------
    # This prevents unnecessary DB connections and makes the job cheaper/faster.
    if df.rdd.isEmpty():
        if logger:
            logger.info("No new records found. Skipping load step.")
        return

    # -------------------------------------------------------------------------
    # 3) Security: hash sensitive identifiers before storing
    # -------------------------------------------------------------------------
    # customer_id and product_id are anonymized so raw IDs are never stored.
    df = encrypt_ids(df)

    # -------------------------------------------------------------------------
    # 4) Concurrency and performance
    # -------------------------------------------------------------------------
    # Spark is already parallel. Repartition controls how many parallel JDBC
    # writes will occur.
    #
    # NOTE:
    # In a real system, we would tune this number based on:
    # - DB capacity
    # - network bandwidth
    # - dataset size
    df = df.repartition(4)

    # -------------------------------------------------------------------------
    # 5) JDBC properties
    # -------------------------------------------------------------------------
    props = {
        "user": cfg["db"]["user"],
        "password": cfg["db"]["password"],
        "driver": cfg["db"]["driver"]
    }

    # -------------------------------------------------------------------------
    # 6) Write to Postgres
    # -------------------------------------------------------------------------
    df.write.jdbc(
        url=cfg["db"]["url"],
        table=cfg["db"]["table"],
        mode="append",
        properties=props
    )

    # -------------------------------------------------------------------------
    # 7) Update watermark after successful load
    # -------------------------------------------------------------------------
    # We update watermark based on the max transaction_id that was actually
    # written in this run.
    max_id = df.agg(F.max("transaction_id").alias("max_id")).collect()[0]["max_id"]

    if max_id:
        write_watermark(state_path, max_id)

        if logger:
            logger.info(f"Watermark updated: last_transaction_id={max_id}")


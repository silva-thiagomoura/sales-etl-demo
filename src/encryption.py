"""
encryption.py

This module handles sensitive data protection.

Important note:
- We are using SHA-256 hashing (one-way) instead of reversible encryption.
- This is intentional for analytics pipelines where we do not need to recover
  the original IDs, only to anonymize them.

Why hashing?
- One-way protection: the original value cannot be recovered.
- Prevents exposing raw customer/product IDs in the database.
- Still allows joins/grouping consistently because the same input always
  produces the same hash output.

In a real production system, you may also apply:
- Salted hashes
- Tokenization
- Key management services (AWS KMS, Azure Key Vault, HashiCorp Vault)
"""

from pyspark.sql import functions as F


def encrypt_ids(df):
    """
    Anonymizes sensitive identifiers using SHA-256 hashing.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing customer_id and product_id columns.

    Returns
    -------
    pyspark.sql.DataFrame
        Output DataFrame where:
        - customer_id is replaced by its SHA-256 hash
        - product_id is replaced by its SHA-256 hash

    Notes
    -----
    - We cast IDs to string to avoid issues if they are numeric.
    - The hashing is deterministic: same ID => same hash.
    - This allows analytics and aggregations without exposing raw identifiers.
    """

    return (
        df.withColumn(
            "customer_id",
            F.sha2(F.col("customer_id").cast("string"), 256)
        )
        .withColumn(
            "product_id",
            F.sha2(F.col("product_id").cast("string"), 256)
        )
    )

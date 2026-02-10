import os
import random
import pandas as pd
from datetime import datetime, timedelta


def generate_mock_sales_csv(output_path: str, n_rows: int = 50000):
    """
    Generates a mock sales CSV file with random transactional data.

    This script is useful for:
    - local testing
    - ETL development
    - simulating incremental loads
    - interview demos without relying on real sensitive data

    Parameters
    ----------
    output_path : str
        Output file path where the CSV will be written.
        Example: "data/raw/sales.csv"

    n_rows : int
        Number of rows to generate.
        Default: 50,000
    """

    # Ensure the parent folder exists (e.g., data/raw)
    # This prevents FileNotFoundError when writing the CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Generate timestamps from the last 10 days
    # This makes the dataset look more realistic (not all same day/time)
    base_time = datetime.now() - timedelta(days=10)

    # We'll build a list of dictionaries (rows), then convert to a DataFrame
    rows = []

    for i in range(n_rows):
        # Generate a sequential transaction ID
        # This is important because we use it later as a watermark
        transaction_id = i + 1

        # Random customer IDs in a realistic range
        customer_id = random.randint(1000, 5000)

        # Random product IDs (catalog size: 300 products)
        product_id = random.randint(1, 300)

        # Quantity of products purchased (1 to 10)
        quantity = random.randint(1, 10)

        # Random timestamp within the last 10 days
        # (0 to 10 days in minutes)
        ts = base_time + timedelta(minutes=random.randint(0, 60 * 24 * 10))

        # Append the row as a dictionary (CSV-friendly format)
        rows.append({
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "timestamp": ts.isoformat()  # ISO format is easy to parse in Spark
        })

    # Convert rows to Pandas DataFrame
    df = pd.DataFrame(rows)

    # Write CSV with header and without index column
    df.to_csv(output_path, index=False)

    print(f"Mock CSV generated: {output_path} ({n_rows} rows)")


if __name__ == "__main__":
    # Entry point for running this script directly
    # Example:
    #   python generate_mock_sales_csv.py
    generate_mock_sales_csv("data/raw/sales.csv", n_rows=50000)

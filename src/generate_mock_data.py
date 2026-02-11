import os
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta


def generate_mock_sales_csv(output_path: str, n_rows: int = 50000, seed: int = 42):
    """
    Generates a realistic mock sales dataset for ETL demos and interviews.

    This dataset simulates real-world e-commerce + retail sales transactions, including:
    - customer info (email for encryption demo)
    - product catalog with realistic pricing
    - discounts, taxes (VAT), shipping costs
    - multiple countries and currencies
    - payment methods
    - order status (paid, refunded, cancelled)
    - sales channel (web/app/store)
    - timestamps spread across the last 30 days
    - realistic dirty data (optional)

    Output format: CSV
    """

    random.seed(seed)

    # Ensure folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # ----------------------------
    # 1) Product Catalog (Mock)
    # ----------------------------
    categories = {
        "electronics": (50, 1200),
        "fashion": (10, 250),
        "home": (15, 600),
        "sports": (10, 400),
        "beauty": (5, 150),
        "books": (5, 60)
    }

    product_catalog = []
    for product_id in range(1, 301):
        category = random.choice(list(categories.keys()))
        min_price, max_price = categories[category]
        base_price = round(random.uniform(min_price, max_price), 2)

        product_catalog.append({
            "product_id": product_id,
            "category": category,
            "base_price": base_price
        })

    product_df = pd.DataFrame(product_catalog)

    # ----------------------------
    # 2) Countries & Currencies
    # ----------------------------
    # Simulating EU + US for realism
    geo = [
        {"country": "ES", "currency": "EUR", "vat_rate": 0.21},
        {"country": "PT", "currency": "EUR", "vat_rate": 0.23},
        {"country": "FR", "currency": "EUR", "vat_rate": 0.20},
        {"country": "DE", "currency": "EUR", "vat_rate": 0.19},
        {"country": "IT", "currency": "EUR", "vat_rate": 0.22},
        {"country": "US", "currency": "USD", "vat_rate": 0.00}
    ]

    channels = ["web", "mobile_app", "store"]
    payment_methods = ["credit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]

    # ----------------------------
    # 3) Time distribution
    # ----------------------------
    # More realistic: last 30 days
    base_time = datetime.now() - timedelta(days=30)

    # ----------------------------
    # 4) Generate rows
    # ----------------------------
    rows = []

    for i in range(n_rows):
        transaction_id = i + 1

        # Create a stable "customer base"
        customer_id = random.randint(1000, 20000)

        # Email for encryption demo
        customer_email = f"customer{customer_id}@example.com"

        # Pick a product from catalog
        product = product_df.sample(1).iloc[0]
        product_id = int(product["product_id"])
        category = product["category"]
        base_price = float(product["base_price"])

        # Quantity: skewed towards 1–3 (more realistic)
        quantity = random.choices([1, 2, 3, 4, 5, 6], weights=[50, 25, 12, 6, 4, 3])[0]

        # Discount probability varies by category
        discount_pct = 0.0
        if category in ["fashion", "beauty"]:
            if random.random() < 0.35:
                discount_pct = random.choice([0.05, 0.10, 0.15, 0.20, 0.30])
        else:
            if random.random() < 0.15:
                discount_pct = random.choice([0.05, 0.10, 0.15])

        # Geo
        g = random.choice(geo)
        country = g["country"]
        currency = g["currency"]
        vat_rate = g["vat_rate"]

        # Channel
        channel = random.choices(channels, weights=[45, 35, 20])[0]

        # Payment method
        payment_method = random.choice(payment_methods)

        # Order status
        # Most are paid, some refunded/cancelled
        status = random.choices(
            ["paid", "refunded", "cancelled"],
            weights=[92, 5, 3]
        )[0]

        # Timestamp: random minute in last 30 days
        ts = base_time + timedelta(minutes=random.randint(0, 60 * 24 * 30))

        # Shipping cost (store = usually 0)
        if channel == "store":
            shipping_cost = 0.0
        else:
            shipping_cost = round(random.choice([0, 2.99, 4.99, 7.99, 9.99]), 2)

        # Unit price variation (promotions / rounding)
        unit_price = round(base_price * random.uniform(0.95, 1.05), 2)

        # Calculations
        gross_amount = round(unit_price * quantity, 2)
        discount_amount = round(gross_amount * discount_pct, 2)
        net_amount = round(gross_amount - discount_amount, 2)

        vat_amount = round(net_amount * vat_rate, 2)
        total_amount = round(net_amount + vat_amount + shipping_cost, 2)

        # Unique order_id (helps realism)
        order_id = str(uuid.uuid4())

        rows.append({
            "transaction_id": transaction_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_email": customer_email,
            "product_id": product_id,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": gross_amount,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "net_amount": net_amount,
            "vat_rate": vat_rate,
            "vat_amount": vat_amount,
            "shipping_cost": shipping_cost,
            "total_amount": total_amount,
            "currency": currency,
            "country": country,
            "channel": channel,
            "payment_method": payment_method,
            "status": status,
            "timestamp": ts.isoformat()
        })

    df = pd.DataFrame(rows)

    # Write CSV
    df.to_csv(output_path, index=False)
    print(f"Realistic mock CSV generated: {output_path} ({n_rows} rows)")


if __name__ == "__main__":
    generate_realistic_mock_sales_csv("data/raw/sales.csv", n_rows=50000)

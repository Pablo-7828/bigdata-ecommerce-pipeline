"""
generate_dataset.py
--------------------
Generates a large synthetic e-commerce transactions dataset for the
Big Data pipeline project. Simulates 1.5M+ order-line records across
multiple regions, categories, and time periods -- large enough to
justify PySpark-based processing instead of pandas.

Usage:
    python generate_dataset.py --rows 1500000 --out data/ecommerce_transactions.csv
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATEGORIES = {
    "Electronics": ["Smartphone", "Laptop", "Headphones", "Smartwatch", "Tablet"],
    "Fashion": ["T-Shirt", "Jeans", "Sneakers", "Jacket", "Saree"],
    "Home & Kitchen": ["Mixer Grinder", "Cookware Set", "Bedsheet", "Air Fryer", "Lamp"],
    "Beauty": ["Moisturizer", "Shampoo", "Lipstick", "Perfume", "Sunscreen"],
    "Sports": ["Cricket Bat", "Yoga Mat", "Dumbbells", "Football", "Cycling Helmet"],
    "Books": ["Fiction Novel", "Self-Help Book", "Textbook", "Comic", "Biography"],
}

REGIONS = {
    "West": ["Pune", "Mumbai", "Ahmedabad", "Surat"],
    "North": ["Delhi", "Jaipur", "Lucknow", "Chandigarh"],
    "South": ["Bangalore", "Chennai", "Hyderabad", "Kochi"],
    "East": ["Kolkata", "Bhubaneswar", "Patna", "Guwahati"],
}

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]


def generate(n_rows: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    category_list = list(CATEGORIES.keys())
    region_list = list(REGIONS.keys())

    categories = rng.choice(category_list, size=n_rows)
    products = [rng.choice(CATEGORIES[c]) for c in categories]
    regions = rng.choice(region_list, size=n_rows)
    cities = [rng.choice(REGIONS[r]) for r in regions]

    start_date = datetime(2024, 1, 1)
    order_dates = [start_date + timedelta(days=int(d)) for d in rng.integers(0, 640, size=n_rows)]

    quantity = rng.integers(1, 6, size=n_rows)
    base_price = rng.normal(1500, 900, size=n_rows).clip(50, 12000).round(2)

    # Inject some data-quality issues on purpose, mirroring real-world messy data
    # so the pipeline has genuine cleaning work to do.
    null_mask = rng.random(n_rows) < 0.015
    base_price[null_mask] = np.nan

    dup_frac = 0.005
    n_dupe = int(n_rows * dup_frac)

    df = pd.DataFrame({
        "order_id": np.arange(1, n_rows + 1),
        "customer_id": rng.integers(100000, 250000, size=n_rows),
        "order_date": order_dates,
        "category": categories,
        "product_name": products,
        "quantity": quantity,
        "unit_price": base_price,
        "region": regions,
        "city": cities,
        "payment_method": rng.choice(PAYMENT_METHODS, size=n_rows,
                                      p=[0.38, 0.22, 0.18, 0.12, 0.10]),
    })

    # Add duplicate rows on purpose (common in real pipelines from retries/logging)
    dupes = df.sample(n=n_dupe, random_state=seed).copy()
    df = pd.concat([df, dupes], ignore_index=True)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_500_000)
    parser.add_argument("--out", type=str, default="data/ecommerce_transactions.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = generate(args.rows, args.seed)
    df.to_csv(args.out, index=False)
    print(f"Wrote {len(df):,} rows to {args.out}")

"""
validate_with_pandas.py
------------------------
This mirrors the exact cleaning/aggregation logic in pyspark_pipeline.py,
implemented in pandas instead of Spark. It exists ONLY to validate the
pipeline logic and produce real output files in environments where
PySpark cannot be installed (e.g. no internet access).

On your own machine, run pyspark_pipeline.py instead — that is the
actual Big Data (Spark) deliverable for your resume/project.
"""

import pandas as pd
import numpy as np

INPUT = "data/ecommerce_transactions.csv"
OUTPUT_DIR = "outputs"


def clean(df):
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["unit_price"])
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_month"] = df["order_date"].dt.strftime("%Y-%m")
    df["revenue"] = (df["unit_price"] * df["quantity"]).round(2)
    df = df[(df["unit_price"] > 0) & (df["quantity"] > 0)]
    return df


def transform(df):
    revenue_by_category = (
        df.groupby("category")
        .agg(total_revenue=("revenue", "sum"), order_count=("order_id", "count"),
             avg_order_value=("revenue", "mean"))
        .round(2)
        .sort_values("total_revenue", ascending=False)
        .reset_index()
    )

    revenue_by_region_month = (
        df.groupby(["region", "order_month"])["revenue"].sum()
        .round(2)
        .reset_index()
        .rename(columns={"revenue": "total_revenue"})
        .sort_values(["region", "order_month"])
    )

    top_products = (
        df.groupby(["category", "product_name"])
        .agg(total_revenue=("revenue", "sum"), units_sold=("order_id", "count"))
        .round(2)
        .sort_values("total_revenue", ascending=False)
        .head(20)
        .reset_index()
    )

    payment_mix = df.groupby("payment_method")["order_id"].count().reset_index()
    payment_mix.columns = ["payment_method", "order_count"]
    payment_mix["pct_of_orders"] = (payment_mix["order_count"] * 100.0 / payment_mix["order_count"].sum()).round(2)
    payment_mix = payment_mix.sort_values("order_count", ascending=False)

    orders_per_customer = df.groupby("customer_id")["order_id"].count()
    segment = pd.cut(
        orders_per_customer,
        bins=[0, 1, 3, np.inf],
        labels=["One-time", "Occasional", "Repeat"],
    )
    customer_segments = segment.value_counts().reset_index()
    customer_segments.columns = ["segment", "customer_count"]

    return {
        "revenue_by_category": revenue_by_category,
        "revenue_by_region_month": revenue_by_region_month,
        "top_products": top_products,
        "payment_mix": payment_mix,
        "customer_segments": customer_segments,
    }


if __name__ == "__main__":
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    raw = pd.read_csv(INPUT)
    print(f"Raw row count: {len(raw):,}")

    cleaned = clean(raw)
    print(f"Cleaned row count: {len(cleaned):,}")

    # Parquet export skipped in sandbox (no pyarrow available offline).
    # pyspark_pipeline.py writes proper partitioned Parquet when run with real Spark.
    cleaned.head(5000).to_csv(f"{OUTPUT_DIR}/cleaned_transactions_sample.csv", index=False)

    summaries = transform(cleaned)
    for name, sdf in summaries.items():
        sdf.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)
        print(f"\n--- {name} ---")
        print(sdf.head(10).to_string(index=False))

    print(f"\nValidation run complete. Outputs written to {OUTPUT_DIR}/")

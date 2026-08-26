"""
pyspark_pipeline.py
--------------------
End-to-end Big Data pipeline for e-commerce transaction analytics.

Pipeline stages:
    1. Ingest raw CSV (1.5M+ rows) with Spark
    2. Clean: drop nulls/duplicates, cast types, derive columns
    3. Transform: aggregate revenue by category / region / month
    4. Load: write cleaned data as partitioned Parquet (Hive-style),
       and write summary tables as CSV for BI tools (Power BI / Tableau)

Run locally (after `pip install pyspark`):
    spark-submit pyspark_pipeline.py \
        --input data/ecommerce_transactions.csv \
        --output outputs/

Or with plain python (PySpark bundles its own runner):
    python pyspark_pipeline.py --input data/ecommerce_transactions.csv --output outputs/
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def build_spark(app_name: str = "EcommerceBigDataPipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")  # tuned for laptop-scale runs
        .getOrCreate()
    )


def load_raw(spark: SparkSession, path: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def clean(df):
    """Data cleaning stage: handle nulls, duplicates, bad types."""
    df = df.dropDuplicates(["order_id"])
    df = df.dropna(subset=["unit_price"])
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("order_date", F.to_date("order_date"))
    df = df.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    df = df.withColumn("revenue", F.round(F.col("unit_price") * F.col("quantity"), 2))
    # Drop obviously bad rows (negative/zero prices, absurd quantities)
    df = df.filter((F.col("unit_price") > 0) & (F.col("quantity") > 0))
    return df


def transform(df):
    """Aggregation stage: build the analytical summary tables."""
    revenue_by_category = (
        df.groupBy("category")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    revenue_by_region_month = (
        df.groupBy("region", "order_month")
        .agg(F.sum("revenue").alias("total_revenue"))
        .orderBy("region", "order_month")
    )

    top_products = (
        df.groupBy("category", "product_name")
        .agg(F.sum("revenue").alias("total_revenue"), F.count("order_id").alias("units_sold"))
        .orderBy(F.desc("total_revenue"))
        .limit(20)
    )

    payment_mix = (
        df.groupBy("payment_method")
        .agg(F.count("order_id").alias("order_count"))
        .withColumn(
            "pct_of_orders",
            F.round(F.col("order_count") * 100.0 / F.sum("order_count").over(Window.partitionBy()), 2),
        )
        .orderBy(F.desc("order_count"))
    )

    customer_window = Window.partitionBy("customer_id")
    repeat_customers = (
        df.withColumn("orders_by_customer", F.count("order_id").over(customer_window))
        .select("customer_id", "orders_by_customer")
        .distinct()
        .withColumn(
            "segment",
            F.when(F.col("orders_by_customer") == 1, "One-time")
            .when(F.col("orders_by_customer") <= 3, "Occasional")
            .otherwise("Repeat"),
        )
        .groupBy("segment")
        .agg(F.count("customer_id").alias("customer_count"))
        .orderBy(F.desc("customer_count"))
    )

    return {
        "revenue_by_category": revenue_by_category,
        "revenue_by_region_month": revenue_by_region_month,
        "top_products": top_products,
        "payment_mix": payment_mix,
        "customer_segments": repeat_customers,
    }


def save_outputs(cleaned_df, summaries: dict, output_dir: str):
    # Cleaned fact table -> partitioned Parquet, Hive-style (query engine ready)
    (
        cleaned_df.write.mode("overwrite")
        .partitionBy("region")
        .parquet(f"{output_dir}/cleaned_transactions_parquet")
    )

    # Summary tables -> single CSV files, ready to plug straight into Power BI
    for name, sdf in summaries.items():
        (
            sdf.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(f"{output_dir}/{name}_csv")
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/ecommerce_transactions.csv")
    parser.add_argument("--output", default="outputs")
    args = parser.parse_args()

    spark = build_spark()
    raw_df = load_raw(spark, args.input)
    print(f"Raw row count: {raw_df.count():,}")

    cleaned_df = clean(raw_df)
    print(f"Cleaned row count: {cleaned_df.count():,}")

    summaries = transform(cleaned_df)
    for name, sdf in summaries.items():
        print(f"\n--- {name} ---")
        sdf.show(10, truncate=False)

    save_outputs(cleaned_df, summaries, args.output)
    print(f"\nPipeline complete. Outputs written to {args.output}/")

    spark.stop()


if __name__ == "__main__":
    main()

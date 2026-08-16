"""
Iceberg workload generation: base table and correction batches.
"""

import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, rand, when, broadcast, year, month, dayofmonth,
    hash
)
import pyspark.sql.types as T


def build_base_table(
    spark: SparkSession,
    table_name: str,
    base_rows: int,
    days: int,
    target_file_mb: int,
    seed: int,
) -> None:
    """
    Build initial fact table with day partitioning and controlled file size.

    The table is partitioned by day, with enough data per day to hit target_file_mb
    when written as Parquet with Iceberg.

    Args:
        spark: SparkSession
        table_name: Full table name (catalog.schema.table)
        base_rows: Total rows to generate
        days: Number of days to span
        target_file_mb: Target file size in MB
        seed: Random seed for reproducibility
    """
    random.seed(seed)

    rows_per_day = base_rows // days

    # Generate base data: day, event_id, customer_id, amount, event_time
    print(f"Generating {base_rows:,} rows across {days} days...")

    base_date = datetime(2024, 1, 1)
    data = []

    for day_offset in range(days):
        current_date = base_date + timedelta(days=day_offset)
        for _ in range(rows_per_day):
            data.append({
                "day": current_date.date(),
                "event_id": random.randint(1_000_000_000, 9_999_999_999),
                "customer_id": random.randint(1, 100_000),
                "amount": round(random.uniform(1.0, 1000.0), 2),
                "event_time": current_date.replace(
                    hour=random.randint(0, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                ),
            })

    schema = T.StructType([
        T.StructField("day", T.DateType(), False),
        T.StructField("event_id", T.LongType(), False),
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("event_time", T.TimestampType(), False),
    ])

    df = spark.createDataFrame(data, schema=schema)

    # Repartition to target file size (rough estimate: 1000 rows ~= 100KB)
    bytes_per_row_estimate = 100
    target_bytes = target_file_mb * 1024 * 1024
    target_rows_per_file = target_bytes // bytes_per_row_estimate
    partitions_needed = max(1, base_rows // target_rows_per_file)

    print(f"Writing {partitions_needed} partitions targeting {target_file_mb}MB files...")

    df = df.repartition(partitions_needed, col("day"))

    # Create or replace table
    df.write.format("iceberg") \
        .partitionedBy("day") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    print(f"Table {table_name} created with {base_rows:,} rows")

    # Verify
    count = spark.table(table_name).count()
    print(f"Verification: {count:,} rows in {table_name}")


def make_correction_batch(
    spark: SparkSession,
    table_name: str,
    batch_idx: int,
    batch_rows: int,
    scatter_parts: int,
    seed: int,
) -> DataFrame:
    """
    Generate a correction batch: sparse rows scattered across historical partitions.

    The correction contains event_id values matching rows in the main table,
    simulating a late-arriving correction that updates historical data.

    Args:
        spark: SparkSession
        table_name: Main table name
        batch_idx: Batch index for seed variation
        batch_rows: Number of rows in this batch
        scatter_parts: Number of distinct days to touch
        seed: Base random seed

    Returns:
        DataFrame with columns (day, event_id, customer_id, amount, corrected_amount)
    """
    random.seed(seed + batch_idx)

    # Sample existing data to get valid event_ids and days
    sample_df = spark.table(table_name).sample(fraction=0.01, seed=seed + batch_idx)
    sample_df = sample_df.select("day", "event_id", "customer_id", "amount").cache()

    # Pick random days from the sample
    days_available = sample_df.select("day").distinct().limit(scatter_parts).collect()
    days_list = [row["day"] for row in days_available]

    if not days_list:
        days_list = [datetime(2024, 1, 1).date()]

    print(f"Correction batch {batch_idx}: {batch_rows} rows across {len(days_list)} partitions")

    # Generate corrections by sampling from existing data
    corrections = []
    for _ in range(batch_rows):
        sample_row = sample_df.sample(fraction=min(1.0, 100.0 / sample_df.count()) if sample_df.count() > 0 else 1.0).limit(1).collect()
        if sample_row:
            row = sample_row[0]
            corrections.append({
                "day": row["day"],
                "event_id": row["event_id"],
                "customer_id": row["customer_id"],
                "amount": row["amount"],
                "corrected_amount": round(row["amount"] * random.uniform(0.9, 1.1), 2),
            })

    if not corrections:
        print("Warning: No corrections generated, creating synthetic batch")
        for i in range(batch_rows):
            corrections.append({
                "day": days_list[i % len(days_list)],
                "event_id": random.randint(1_000_000_000, 9_999_999_999),
                "customer_id": random.randint(1, 100_000),
                "amount": 100.0,
                "corrected_amount": 110.0,
            })

    schema = T.StructType([
        T.StructField("day", T.DateType(), False),
        T.StructField("event_id", T.LongType(), False),
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("corrected_amount", T.DoubleType(), False),
    ])

    return spark.createDataFrame(corrections, schema=schema)


def apply_correction_batch(
    spark: SparkSession,
    table_name: str,
    corrections_df: DataFrame,
) -> None:
    """
    Apply correction batch via MERGE (updates become delete+insert).

    On v2 format: creates positional deletes.
    On v3 format: creates deletion vectors.

    Args:
        spark: SparkSession
        table_name: Target table
        corrections_df: DataFrame with corrections (day, event_id, customer_id, amount, corrected_amount)
    """
    spark.sql(f"""
    MERGE INTO {table_name} t
    USING (SELECT * FROM __correction) c
    ON t.event_id = c.event_id AND t.day = c.day
    WHEN MATCHED THEN UPDATE SET amount = c.corrected_amount
    WHEN NOT MATCHED THEN INSERT (day, event_id, customer_id, amount, event_time)
        VALUES (c.day, c.event_id, c.customer_id, c.corrected_amount, CURRENT_TIMESTAMP())
    """)

    # Register corrections as temp view for the merge
    corrections_df.createOrReplaceTempView("__correction")


if __name__ == "__main__":
    from iceberg_benchmark.config import get_config
    cfg = get_config()
    from iceberg_benchmark.spark_session import create_spark_session

    spark = create_spark_session(cfg.warehouse_path)

    build_base_table(
        spark,
        "iceberg.benchmark.benchmark_test",
        base_rows=100_000,
        days=10,
        target_file_mb=64,
        seed=cfg.random_seed,
    )

    batch = make_correction_batch(
        spark,
        "iceberg.benchmark.benchmark_test",
        batch_idx=0,
        batch_rows=5_000,
        scatter_parts=5,
        seed=cfg.random_seed,
    )

    print(f"Generated batch with {batch.count()} rows")

    spark.stop()

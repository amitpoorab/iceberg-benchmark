"""
Measurement utilities: query latency, compaction cost, delete file tracking.
"""

import time
from typing import List, Dict, Any
from statistics import median
from pyspark.sql import SparkSession


def query_full_scan(spark: SparkSession, table_name: str) -> float:
    """
    Execute full table scan and return latency in milliseconds.

    Args:
        spark: SparkSession
        table_name: Table to scan

    Returns:
        Latency in milliseconds
    """
    start = time.time()
    spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms


def query_selective(
    spark: SparkSession,
    table_name: str,
    fraction: float = 0.05,
) -> float:
    """
    Execute selective scan (WHERE clause filtering ~fraction of rows).

    Args:
        spark: SparkSession
        table_name: Table to scan
        fraction: Approximate fraction of rows to filter (0-1)

    Returns:
        Latency in milliseconds
    """
    # Predicate: amount > some percentile
    percentile_val = int(100 * (1 - fraction))
    start = time.time()
    spark.sql(
        f"SELECT COUNT(*) FROM {table_name} "
        f"WHERE amount > (SELECT PERCENTILE_APPROX(amount, {(1-fraction)}) FROM {table_name})"
    ).collect()
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms


def measure_query_latency(
    spark: SparkSession,
    table_name: str,
    warmup: int = 3,
    measured: int = 5,
    selective_fraction: float = 0.05,
) -> Dict[str, float]:
    """
    Measure query latencies: warmup runs discarded, median of measured runs.

    Returns:
        {
            "full_scan_ms": median_latency,
            "selective_ms": median_latency,
        }
    """
    print(f"Measuring query latency ({warmup} warmup, {measured} measured)...")

    # Warmup
    for _ in range(warmup):
        query_full_scan(spark, table_name)
        query_selective(spark, table_name, selective_fraction)

    # Measured runs
    full_scan_times = []
    selective_times = []

    for _ in range(measured):
        full_scan_times.append(query_full_scan(spark, table_name))
        selective_times.append(query_selective(spark, table_name, selective_fraction))

    full_scan_median = median(full_scan_times)
    selective_median = median(selective_times)

    print(f"  Full scan: {full_scan_median:.1f}ms")
    print(f"  Selective: {selective_median:.1f}ms")

    return {
        "full_scan_ms": full_scan_median,
        "selective_ms": selective_median,
    }


def compact_table(
    spark: SparkSession,
    table_name: str,
) -> Dict[str, Any]:
    """
    Compact table with rewrite_data_files and track cost.

    Returns:
        {
            "duration_ms": wall_clock_time,
            "files_rewritten": count,
            "bytes_rewritten": total_bytes,
        }
    """
    print(f"Compacting {table_name}...")
    start = time.time()

    result = spark.sql(f"CALL system.rewrite_data_files('{table_name}')").collect()

    duration_ms = (time.time() - start) * 1000

    # Parse result
    files_rewritten = 0
    bytes_rewritten = 0
    if result:
        row = result[0]
        files_rewritten = row[1] if len(row) > 1 else 0
        bytes_rewritten = row[3] if len(row) > 3 else 0

    print(f"  Duration: {duration_ms:.0f}ms, Files: {files_rewritten}, Bytes: {bytes_rewritten:,}")

    return {
        "duration_ms": duration_ms,
        "files_rewritten": files_rewritten,
        "bytes_rewritten": bytes_rewritten,
    }


def table_delete_stats(
    spark: SparkSession,
    table_name: str,
) -> Dict[str, int]:
    """
    Query table metadata to get delete file and data file counts.

    Reads from the .files metadata table to avoid expensive table scan.

    Returns:
        {
            "data_files": count,
            "delete_files": count,
        }
    """
    try:
        # Count files by type in the metadata
        files_df = spark.sql(f"SELECT file_path FROM {table_name}.files")
        files_df.createOrReplaceTempView("__files")

        data_files = spark.sql(
            "SELECT COUNT(*) as cnt FROM __files WHERE file_path LIKE '%.parquet'"
        ).collect()[0]["cnt"]

        delete_files = spark.sql(
            "SELECT COUNT(*) as cnt FROM __files WHERE file_path LIKE '%.parquet%' AND file_path LIKE '%delete%'"
        ).collect()[0]["cnt"]

    except Exception as e:
        print(f"Warning: Could not query delete stats: {e}")
        data_files = 0
        delete_files = 0

    print(f"  Data files: {data_files}, Delete files: {delete_files}")

    return {
        "data_files": data_files,
        "delete_files": delete_files,
    }


def set_format_version(
    spark: SparkSession,
    table_name: str,
    version: int,
) -> None:
    """
    Set Iceberg format version (2 or 3).

    Version 2: Positional deletes
    Version 3: Deletion vectors

    Args:
        spark: SparkSession
        table_name: Table name
        version: 2 or 3
    """
    print(f"Setting {table_name} to format version {version}...")
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '{version}')")
    print(f"  Format version set to {version}")


if __name__ == "__main__":
    from iceberg_benchmark.config import get_config
    from iceberg_benchmark.spark_session import create_spark_session
    from iceberg_benchmark.workload import build_base_table

    cfg = get_config()
    spark = create_spark_session(cfg.warehouse_path)

    test_table = "spark_catalog.default.measurement_test"

    build_base_table(
        spark,
        test_table,
        base_rows=100_000,
        days=10,
        target_file_mb=64,
        seed=cfg.random_seed,
    )

    latency = measure_query_latency(spark, test_table)
    print(f"Query latency: {latency}")

    compact_result = compact_table(spark, test_table)
    print(f"Compact result: {compact_result}")

    stats = table_delete_stats(spark, test_table)
    print(f"Delete stats: {stats}")

    spark.stop()

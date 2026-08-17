"""
Spark session initialization with Iceberg catalog and proper configuration.
Auto-optimize and auto-compaction are disabled to enable manual compaction measurement.
"""

import os
from pyspark.sql import SparkSession


def create_spark_session(warehouse_path: str) -> SparkSession:
    """
    Create a Spark session configured for Iceberg benchmarking.

    Args:
        warehouse_path: Path to warehouse (local FS or S3)

    Returns:
        Configured SparkSession with Iceberg Hadoop catalog
    """
    profile = os.getenv("BENCH_PROFILE", "local").lower()

    spark_builder = (
        SparkSession.builder
        .appName("IcebergBenchmark")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_path)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        # Disable auto-optimization to measure compaction explicitly
        .config("spark.sql.iceberg.optimize.enabled", "false")
        .config("spark.sql.iceberg.optimize.min-input-files", "0")
        .config("spark.sql.iceberg.auto-optimize.enabled", "false")
        .config("spark.sql.iceberg.auto-compact.enabled", "false")
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
    )

    if profile == "ec2":
        # S3A configuration for EC2 with instance profile credentials
        spark_builder = (
            spark_builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        )

    spark = spark_builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == "__main__":
    from iceberg_benchmark.config import get_config
    cfg = get_config()
    spark = create_spark_session(cfg.warehouse_path)
    print(f"Spark session created: {spark.version}")
    print(f"Catalog: {spark.catalog.currentCatalog()}")
    spark.stop()

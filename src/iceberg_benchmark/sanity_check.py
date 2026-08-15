"""
Sanity check: apply ~10 batches with NO compaction, verify read degradation >= 15%.
Fails loudly if measurement would be noise.
"""

import sys
from pyspark.sql import SparkSession
from iceberg_benchmark.config import BenchmarkConfig, get_config
from iceberg_benchmark.spark_session import create_spark_session
from iceberg_benchmark.workload import build_base_table, make_correction_batch, apply_correction_batch
from iceberg_benchmark.measure import measure_query_latency, table_delete_stats, set_format_version


def run_sanity_check(cfg: BenchmarkConfig, spark: SparkSession) -> bool:
    """
    Run sanity check: no compaction, measure degradation.

    Returns:
        True if degradation >= 15%, False otherwise
    """
    test_table = "iceberg.benchmark.sanity_check_table"

    print("=" * 70)
    print("SANITY CHECK: Late-arriving data read degradation (no compaction)")
    print("=" * 70)

    # Build small base table
    small_rows = cfg.base_rows // 10
    print(f"\n1. Creating test table ({small_rows:,} rows)...")
    build_base_table(
        spark,
        test_table,
        base_rows=small_rows,
        days=cfg.days,
        target_file_mb=cfg.target_file_mb,
        seed=cfg.random_seed,
    )

    # Set v2 format
    set_format_version(spark, test_table, 2)

    # Measure baseline
    print("\n2. Measuring baseline latency (no deletes)...")
    baseline = measure_query_latency(
        spark,
        test_table,
        warmup=cfg.warmup_queries,
        measured=cfg.measured_queries,
        selective_fraction=cfg.selective_predicate_fraction,
    )
    baseline_latency = baseline["full_scan_ms"]

    # Apply batches WITHOUT compaction
    batches_to_apply = min(10, cfg.batches_per_cell)
    print(f"\n3. Applying {batches_to_apply} correction batches (NO compaction)...")

    for batch_idx in range(batches_to_apply):
        corrections = make_correction_batch(
            spark,
            test_table,
            batch_idx=batch_idx,
            batch_rows=cfg.batch_rows,
            scatter_parts=cfg.scatter_parts,
            seed=cfg.random_seed,
        )
        apply_correction_batch(spark, test_table, corrections)

        stats = table_delete_stats(spark, test_table)
        print(f"   Batch {batch_idx+1}: {stats['delete_files']} delete files accumulated")

    # Measure degraded latency
    print("\n4. Measuring degraded latency (with accumulated deletes)...")
    degraded = measure_query_latency(
        spark,
        test_table,
        warmup=cfg.warmup_queries,
        measured=cfg.measured_queries,
        selective_fraction=cfg.selective_predicate_fraction,
    )
    degraded_latency = degraded["full_scan_ms"]

    # Check degradation
    degradation_pct = ((degraded_latency - baseline_latency) / baseline_latency) * 100

    print(f"\n5. Results:")
    print(f"   Baseline latency: {baseline_latency:.1f}ms")
    print(f"   Degraded latency: {degraded_latency:.1f}ms")
    print(f"   Degradation: {degradation_pct:.1f}%")

    # Cleanup
    spark.sql(f"DROP TABLE {test_table}")

    # Verdict
    print("\n" + "=" * 70)
    if degradation_pct >= 15.0:
        print("✓ PASS: Read degradation >= 15%. Benchmark will measure signal.")
        print("=" * 70)
        return True
    else:
        print("✗ FAIL: Read degradation < 15%. Measurement would be noise.")
        print("  → Scale up: increase base_rows or batches_per_cell in config.py")
        print("=" * 70)
        return False


if __name__ == "__main__":
    cfg = get_config()
    spark = create_spark_session(cfg.warehouse_path)

    try:
        success = run_sanity_check(cfg, spark)
        spark.stop()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ SANITY CHECK FAILED: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

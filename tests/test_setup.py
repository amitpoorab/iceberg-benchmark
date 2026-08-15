"""
Quick validation that all modules import correctly.
"""

import sys

print("Testing imports...")

try:
    print("  config...", end=" ")
    from iceberg_benchmark.config import get_config
    cfg = get_config()
    print("✓")

    print("  spark_session...", end=" ")
    from iceberg_benchmark.spark_session import create_spark_session
    print("✓")

    print("  workload...", end=" ")
    from iceberg_benchmark.workload import build_base_table, make_correction_batch, apply_correction_batch
    print("✓")

    print("  measure...", end=" ")
    from iceberg_benchmark.measure import (
        query_full_scan, measure_query_latency, compact_table,
        table_delete_stats, set_format_version
    )
    print("✓")

    print("  run_sweep...", end=" ")
    from iceberg_benchmark.run_sweep import run_full_sweep
    print("✓")

    print("  plot...", end=" ")
    from iceberg_benchmark.plot import load_summary, find_optimal_cadence
    print("✓")

    print("  sanity_check...", end=" ")
    from iceberg_benchmark.sanity_check import run_sanity_check
    print("✓")

    print("\n✓ All modules import successfully!")
    print(f"\nConfiguration ({cfg.profile}):")
    print(f"  Base rows: {cfg.base_rows:,}")
    print(f"  Warehouse: {cfg.warehouse_path}")
    print(f"  Batches per cell: {cfg.batches_per_cell}")
    print(f"  Cadences: {cfg.cadences}")

except Exception as e:
    print(f"✗\n\nError: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

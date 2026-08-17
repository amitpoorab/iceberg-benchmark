"""
Main benchmark sweep driver.
For each (mode × cadence): rebuild table, set format version, apply batches with compaction,
record per-batch metrics.
"""

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from pyspark.sql import SparkSession

from iceberg_benchmark.config import BenchmarkConfig, get_config
from iceberg_benchmark.spark_session import create_spark_session
from iceberg_benchmark.workload import build_base_table, make_correction_batch, apply_correction_batch
from iceberg_benchmark.measure import (
    measure_query_latency,
    compact_table,
    table_delete_stats,
    set_format_version,
)


def cleanup_table(spark: SparkSession, table_name: str) -> None:
    """Drop table if it exists."""
    try:
        spark.sql(f"DROP TABLE {table_name}")
    except Exception:
        pass


def run_one_sweep_cell(
    spark: SparkSession,
    cfg: BenchmarkConfig,
    delete_mode: str,
    cadence: int,
    results_dir: Path,
    per_batch_csv_path: Path = None,
    start_time: float = None,
) -> List[Dict[str, Any]]:
    """
    Run one (delete_mode, cadence) cell of the sweep.

    Returns:
        List of per-batch results
    """
    table_name = f"iceberg.benchmark.benchmark_{delete_mode}_{cadence}"
    format_version = 3 if delete_mode == "v3" else 2

    print(f"\n{'='*70}")
    print(f"Cell: mode={delete_mode} (v{format_version}), cadence={cadence}")
    print(f"{'='*70}")

    # Cleanup previous run
    cleanup_table(spark, table_name)

    # Build base table
    print(f"1. Building base table ({cfg.base_rows:,} rows)...")
    build_base_table(
        spark,
        table_name,
        base_rows=cfg.base_rows,
        days=cfg.days,
        target_file_mb=cfg.target_file_mb,
        seed=cfg.random_seed,
    )

    # Set format version
    set_format_version(spark, table_name, format_version)

    # Apply batches with compaction on schedule
    print(f"2. Applying {cfg.batches_per_cell} batches (compacting every {cadence} batch)...")
    per_batch_results = []
    if per_batch_csv_path is None:
        per_batch_csv_path = results_dir / "per_batch.csv"

    for batch_idx in range(cfg.batches_per_cell):
        # Apply correction
        corrections = make_correction_batch(
            spark,
            table_name,
            batch_idx=batch_idx,
            batch_rows=cfg.batch_rows,
            scatter_parts=cfg.scatter_parts,
            seed=cfg.random_seed,
        )
        apply_correction_batch(spark, table_name, corrections)

        # Measure query latency
        latency = measure_query_latency(
            spark,
            table_name,
            warmup=cfg.warmup_queries,
            measured=cfg.measured_queries,
            selective_fraction=cfg.selective_predicate_fraction,
        )

        # Get delete accumulation stats
        stats = table_delete_stats(spark, table_name)

        # Compact if on schedule (every `cadence` batches)
        compact_result = {"duration_ms": 0.0, "files_rewritten": 0, "bytes_rewritten": 0}
        if cadence != float('inf') and (batch_idx + 1) % cadence == 0:
            print(f"   Batch {batch_idx+1}: Compacting...")
            compact_result = compact_table(spark, table_name)

        # Record result with timestamps
        elapsed_min = (time.time() - start_time) / 60 if start_time else 0
        wall_clock_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "timestamp_str": wall_clock_time,
            "timestamp_min": round(elapsed_min, 2),
            "batch_idx": batch_idx,
            "mode": delete_mode,
            "format_version": format_version,
            "cadence": cadence,
            "full_scan_ms": latency["full_scan_ms"],
            "selective_ms": latency["selective_ms"],
            "data_files": stats["data_files"],
            "delete_files": stats["delete_files"],
            "compact_duration_ms": compact_result["duration_ms"],
            "compact_files_rewritten": compact_result["files_rewritten"],
            "compact_bytes_rewritten": compact_result["bytes_rewritten"],
        }

        per_batch_results.append(result)

        # Write incrementally to per_batch.csv (append mode)
        if batch_idx == 0 and not per_batch_csv_path.exists():
            with open(per_batch_csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=result.keys())
                writer.writeheader()
                writer.writerow(result)
        else:
            with open(per_batch_csv_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=result.keys())
                writer.writerow(result)

        if (batch_idx + 1) % 10 == 0:
            print(f"   Batch {batch_idx+1}: full_scan={latency['full_scan_ms']:.0f}ms, "
                  f"delete_files={stats['delete_files']}, "
                  f"compact={compact_result['duration_ms']:.0f}ms")

    # Cleanup
    cleanup_table(spark, table_name)

    return per_batch_results


def summarize_cell(per_batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize a sweep cell: compute total costs."""
    if not per_batch_results:
        return {}

    latencies = [r["full_scan_ms"] for r in per_batch_results]
    compact_times = [r["compact_duration_ms"] for r in per_batch_results]

    total_compact_ms = sum(compact_times)
    total_read_penalty_ms = sum(latencies) - (latencies[0] * len(latencies))  # Penalty above baseline

    summary = {
        "mode": per_batch_results[0]["mode"],
        "format_version": per_batch_results[0]["format_version"],
        "cadence": per_batch_results[0]["cadence"],
        "batches": len(per_batch_results),
        "avg_read_latency_ms": sum(latencies) / len(latencies),
        "total_read_penalty_ms": total_read_penalty_ms,
        "total_compact_ms": total_compact_ms,
        "total_cost_ms": total_read_penalty_ms + total_compact_ms,
        "max_delete_files": max(r["delete_files"] for r in per_batch_results),
    }

    return summary


def run_full_sweep(cfg: BenchmarkConfig, spark: SparkSession, results_dir: Path, smoke: bool = False) -> None:
    """
    Run full benchmark sweep.

    Args:
        cfg: Benchmark configuration
        spark: SparkSession
        results_dir: Directory to write results CSVs
        smoke: If True, run 2 cadences × 1 mode × 12 batches for validation
    """
    results_dir.mkdir(parents=True, exist_ok=True)

    # Determine sweep axes
    cadences = cfg.cadences if not smoke else cfg.cadences[:2]
    modes = cfg.delete_modes if not smoke else ["v2"]
    batches = cfg.batches_per_cell if not smoke else min(12, cfg.batches_per_cell)

    print(f"\nStarting benchmark sweep (smoke={smoke})")
    print(f"  Cadences: {cadences}")
    print(f"  Modes: {modes}")
    print(f"  Batches per cell: {batches}")

    # Generate timestamped filenames (never overwrites previous runs)
    sweep_start_time = time.time()
    timestamp_str = datetime.fromtimestamp(sweep_start_time).strftime("%Y-%m-%d_%H-%M-%S")
    summary_csv = results_dir / f"summary_{timestamp_str}.csv"
    per_batch_csv = results_dir / f"per_batch_{timestamp_str}.csv"

    print(f"\nResults will be saved to:")
    print(f"  {summary_csv.name}")
    print(f"  {per_batch_csv.name}")

    cell_count = 0

    for delete_mode in modes:
        for cadence in cadences:
            # Run sweep cell
            per_batch = run_one_sweep_cell(spark, cfg, delete_mode, cadence, results_dir,
                                           per_batch_csv_path=per_batch_csv, start_time=sweep_start_time)

            # Summarize
            summary = summarize_cell(per_batch)
            elapsed_min = (time.time() - sweep_start_time) / 60
            wall_clock_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
            summary["timestamp_str"] = wall_clock_time
            summary["timestamp_min"] = round(elapsed_min, 2)
            cell_count += 1

            print(f"\nCell Summary ({cell_count}/{len(modes)*len(cadences)}) @ {elapsed_min:.1f}min:")
            print(f"  Total cost: {summary['total_cost_ms']:.0f}ms "
                  f"(read_penalty={summary['total_read_penalty_ms']:.0f}ms, "
                  f"compact={summary['total_compact_ms']:.0f}ms)")

            # Write summary incrementally (append mode)
            if cell_count == 1 and not summary_csv.exists():
                with open(summary_csv, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=summary.keys())
                    writer.writeheader()
                    writer.writerow(summary)
            else:
                with open(summary_csv, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=summary.keys())
                    writer.writerow(summary)

            print(f"  ✓ Results available in results/summary.csv (run analyze.py to see trends)")

    # Write CSVs
    print(f"\n{'='*70}")
    print("Writing results...")

    print(f"\nResults written to {results_dir}")


def main():
    parser = argparse.ArgumentParser(description="Iceberg compaction-cadence benchmark")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Quick validation mode: 2 cadences × 1 mode × 12 batches"
    )

    args = parser.parse_args()

    cfg = get_config()
    results_dir = Path("results")

    print("Iceberg Compaction-Cadence Benchmark")
    print(f"Profile: {cfg.profile}")
    print(f"Warehouse: {cfg.warehouse_path}")

    spark = create_spark_session(cfg.warehouse_path)

    try:
        run_full_sweep(cfg, spark, results_dir, smoke=args.smoke)
        print("\n✓ Sweep complete")
    except Exception as e:
        print(f"\n✗ Sweep failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

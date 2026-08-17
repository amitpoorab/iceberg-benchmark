"""
Benchmark configuration — single source of truth for all knobs.
BENCH_PROFILE env var (local|ec2) flips scale and warehouse path only.
"""

import os
from dataclasses import dataclass
from typing import List


@dataclass
class BenchmarkConfig:
    """Configuration for Iceberg compaction-cadence benchmark."""

    # Profile selection (local for debugging, ec2 for production)
    profile: str

    # Data scale
    base_rows: int
    days: int
    batches_per_cell: int

    # Workload parameters
    batch_rows: int
    scatter_parts: int
    target_file_mb: int

    # Warehouse path (local FS or S3)
    warehouse_path: str

    # Sweep parameters
    cadences: List[int]  # Compaction interval in batches
    delete_modes: List[str]  # ["v2", "v3"]

    # Measurement parameters
    warmup_queries: int
    measured_queries: int
    selective_predicate_fraction: float

    # Random seed (deterministic)
    random_seed: int


def get_config() -> BenchmarkConfig:
    """Load configuration from environment."""
    profile = os.getenv("BENCH_PROFILE", "local").lower()

    if profile == "local":
        return BenchmarkConfig(
            profile="local",
            base_rows=100_000,  # Small for quick testing
            days=10,
            batches_per_cell=12,  # Reduced for smoke testing
            batch_rows=500,  # Smaller batches
            scatter_parts=3,
            target_file_mb=64,
            warehouse_path="/tmp/iceberg_benchmark",
            cadences=[1, 6, 24, 168],  # Every batch, every 6 hrs, daily, weekly
            delete_modes=["v2", "v3"],
            warmup_queries=1,  # Reduced warmup
            measured_queries=2,  # Reduced measured runs
            selective_predicate_fraction=0.05,
            random_seed=42,
        )
    elif profile == "ec2":
        s3_bucket = os.getenv("S3_BUCKET", "iceberg-benchmark")
        return BenchmarkConfig(
            profile="ec2",
            base_rows=50_000_000,
            days=30,
            batches_per_cell=168,
            batch_rows=2000,
            scatter_parts=50,
            target_file_mb=256,
            warehouse_path=f"s3a://{s3_bucket}/warehouse",
            cadences=[1, 6, 24, 168], # , 168],
            delete_modes=["v2", "v3"],
            warmup_queries=3,
            measured_queries=5,
            selective_predicate_fraction=0.05,
            random_seed=42,
        )
    else:
        raise ValueError(f"Unknown BENCH_PROFILE: {profile}. Must be 'local' or 'ec2'.")


if __name__ == "__main__":
    cfg = get_config()
    print(f"Profile: {cfg.profile}")
    print(f"Base rows: {cfg.base_rows:,}")
    print(f"Warehouse: {cfg.warehouse_path}")
    print(f"Cadences: {cfg.cadences}")
    print(f"Delete modes: {cfg.delete_modes}")

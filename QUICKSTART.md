# Benchmark Quick Start

Your benchmark is ready to run. Here's the fastest path to first results.

## Setup (1 minute)

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the package in development mode
pip install --upgrade pip
pip install -e .
```

This installs the `iceberg-benchmark` package and PySpark 3.5.0. Iceberg JARs are downloaded at runtime by Spark.

## Run Smoke Test (5 minutes)

Validates the full pipeline locally without waiting for full benchmark:

```bash
export BENCH_PROFILE=local
iceberg-benchmark --smoke
```

This runs **2 cadences × 1 mode × 12 batches** and writes:
- `results/per_batch.csv` — Per-batch metrics
- `results/summary.csv` — Aggregated costs

Expected output: Cost curves showing why compaction cadence matters.

## View Results

```bash
iceberg-plot
```

Prints ASCII summary + gnuplot script for visualization.

## Full Benchmark (1 hour on EC2)

Once smoke test works:

```bash
# On EC2 instance
export BENCH_PROFILE=ec2
iceberg-sanity-check   # Pre-flight check (must pass)
iceberg-benchmark      # Full sweep
iceberg-plot           # Visualize
```

Fill `GROUND_TRUTH.md` with your predictions BEFORE running, so results are scored.

## Architecture at a Glance

| Module | Purpose |
|--------|---------|
| `src/iceberg_benchmark/config.py` | Knobs: scale (local/ec2), batches, cadences |
| `src/iceberg_benchmark/spark_session.py` | Spark 3.5 + Iceberg with manual compaction only |
| `src/iceberg_benchmark/workload.py` | Base table + correction batches (late arrivals) |
| `src/iceberg_benchmark/measure.py` | Query latency, compaction cost, delete file tracking |
| `src/iceberg_benchmark/run_sweep.py` | Sweep driver: (mode × cadence) → per_batch.csv |
| `src/iceberg_benchmark/plot.py` | Visualize cost curves |
| `tests/` | Module validation and integration tests |

## Key Files

- **results/per_batch.csv** — Raw data (one row per batch)
- **results/summary.csv** — Aggregated costs per (mode, cadence)
- **GROUND_TRUTH.md** — Your predictions (update before running full sweep)
- **README.md** — Full documentation

## Troubleshooting

**Q: "data source: iceberg not found"**  
A: Activate the venv: `source venv/bin/activate`, then ensure installed: `pip install -e .`

**Q: "command not found: iceberg-benchmark"**  
A: Activate the venv: `source venv/bin/activate`

**Q: "Failed to find data source: iceberg" at runtime**  
A: This is normal on first run—Spark is downloading Iceberg JARs. Just wait and retry.

**Q: "Read degradation < 15%. Measurement would be noise."**  
A: Scale up in `src/iceberg_benchmark/config.py`: increase `base_rows` or `batches_per_cell`

**Q: v2 and v3 delete-file counts identical**  
A: Format-version toggle not taking. Check Iceberg version / Spark config.

## What This Measures

**The claim:** Total cost = read degradation + compaction cost forms a U-shaped curve.

- **Left side of U:** Low cadence (compact frequently) = high compaction cost
- **Bottom of U:** Optimal cadence C* = minimum total cost
- **Right side of U:** High cadence (compact rarely) = high read degradation

We test whether v3 deletion vectors **shift or flatten** that curve compared to v2.

## Next Steps

1. Run `iceberg-benchmark --smoke` 
2. Check `results/summary.csv`
3. If signal is clear, run full sweep on EC2
4. Publish results to GitHub for peer review

## Running Tests

```bash
# Validate module imports
python -m pytest tests/test_setup.py -v

# Run integration test (requires Spark)
python -m pytest tests/test_integration.py -v
```

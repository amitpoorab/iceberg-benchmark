# Iceberg Compaction-Cadence Benchmark

Measures the cost curve of compaction cadence for late-arriving-data corrections on Apache Iceberg, comparing **v2 positional deletes** against **v3 deletion vectors**.

## The Claim

Total cost = accumulated read degradation + compaction cost forms a **U-shaped curve** in compaction cadence. The minimum is **C*** (optimal cadence). We test whether v3 deletion vectors shift and/or flatten that curve compared to v2.

## Architecture

- `config.py` — Single source of truth. `BENCH_PROFILE` env var (`local`|`ec2`) flips scale and warehouse path only; same code both ways.
- `spark_session.py` — Spark+Iceberg session with **auto-compaction disabled** (manual only).
- `workload.py` — `build_base_table()` and `make_correction_batch()` (sparse rows scattered across partitions).
- `measure.py` — Query latency, compaction cost, delete file tracking.
- `sanity_check.py` — Pre-flight: apply ~10 batches with no compaction, fail if degradation < 15% (noise floor).
- `run_sweep.py` — Main driver. Outputs `results/per_batch.csv` and `results/summary.csv`.
- `plot.py` — Visualize cost curves from results.

## Prerequisites

```bash
# Python 3.10+ and Java 17+ already installed in your environment
python3 --version    # Should be 3.10+
java -version        # Should be 17+

# Install Iceberg package (one-time setup)
pip install iceberg-spark==1.11.0
```

Your environment already has PySpark 3.5.0 installed. Just add iceberg-spark and you're ready.

## Quick Start (Local)

### 1. Smoke Test (2 cadences × 1 mode × 12 batches)

Validates the pipeline cheaply on your machine:

```bash
export BENCH_PROFILE=local
python run_sweep.py --smoke
```

Expected runtime: ~5–10 minutes.

Expected output:
- `results/per_batch.csv` — Per-batch metrics (latency, delete files, compaction cost)
- `results/summary.csv` — Aggregated cost by (mode, cadence)

### 2. Run Full Sweep (Local)

Full benchmark at local scale (5M rows):

```bash
export BENCH_PROFILE=local
python run_sweep.py
```

Expected runtime: ~30–60 minutes.

### 3. Plot Results

```bash
python plot.py
```

Outputs ASCII summary and gnuplot script.

## EC2 Production Run

### 1. Sanity Check (EC2, 200M rows)

Before the full sweep, run the sanity check to ensure read degradation is measurable:

```bash
export BENCH_PROFILE=ec2
python sanity_check.py
```

Must pass (degradation ≥ 15%) before proceeding. If it fails, scaling is insufficient.

### 2. Fill Ground Truth

Edit `GROUND_TRUTH.md` with your predicted C* (optimal cadence) and cost for v2 and v3:

```bash
# Before running the sweep, commit your predictions:
# v2: C* = 6 batches, cost = 50000ms
# v3: C* = 12 batches, cost = 40000ms
```

This lets results be scored right/wrong, not just observed.

### 3. Full Sweep (EC2)

```bash
export BENCH_PROFILE=ec2
python run_sweep.py
```

Expected runtime: ~4–8 hours on a typical EC2 instance (depends on I/O, CPU).

### 4. Terminate EC2 Instance

```bash
# Remember to shut down the instance when done
aws ec2 terminate-instances --instance-ids <id> --region <region>
```

## Understanding Results

### per_batch.csv

Per-batch row includes:
- `batch_idx` — Batch number (0-indexed)
- `mode` — "v2" or "v3"
- `format_version` — Iceberg format version
- `cadence` — Compaction cadence in batches
- `full_scan_ms` — Full table scan latency (median of measured runs)
- `selective_ms` — ~5% selective scan latency
- `data_files` — Current data file count
- `delete_files` — Current delete file count
- `compact_duration_ms` — Wall-clock time for compaction (0 if not triggered this batch)
- `compact_files_rewritten` — Files touched by compaction
- `compact_bytes_rewritten` — Bytes rewritten

### summary.csv

Aggregated by (mode, cadence, cadence):
- `total_read_penalty_ms` — Sum of read latencies above baseline
- `total_compact_ms` — Sum of compaction times
- `total_cost_ms` — Read penalty + compaction (the U-curve y-axis)
- `max_delete_files` — Peak delete file count

The optimal cadence C* is the cadence with minimum `total_cost_ms`.

## Measurement Discipline

This benchmark follows **Gunnar Morling's Hardwood methodology** for credibility:

- ✓ Warmup runs discarded (first 3 queries)
- ✓ Median of measured runs (5 queries), not mean
- ✓ Fixed pinned query set (full scan + 5% selective)
- ✓ Deterministic seed (seed=42)
- ✓ Raw per-batch CSVs published (not just the plot)
- ✓ Relative stability: expect ±3% across runs

## Important Correctness Check

**Before trusting v2 vs v3 comparison:** Verify that format-version toggle actually works.

After a sweep cell completes, check `per_batch.csv`:
- v2 delete-file counts should stay low (positional deletes are compact)
- v3 delete-file counts should be similar or grow more (deletion vectors accumulate)
- If v2 and v3 delete-file counts are identical, format-version toggle did not take — investigate Iceberg config

## File Layout

```
benchmark/
├── config.py              # Configuration
├── spark_session.py       # Spark/Iceberg setup
├── workload.py            # Table and batch generation
├── measure.py             # Query and compaction measurement
├── sanity_check.py        # Pre-flight validation
├── run_sweep.py           # Main benchmark driver
├── plot.py                # Visualization
├── README.md              # This file
├── GROUND_TRUTH.md        # Predictions (fill before running)
└── results/               # Output CSVs and plots
    ├── per_batch.csv
    ├── summary.csv
    └── plot.gnuplot
```

## Troubleshooting

### "Read degradation < 15%. Measurement would be noise."

Your scale is too small. In `config.py`, increase:
- `base_rows` (local: 5M → 20M; ec2: 200M → 500M)
- `batches_per_cell` (168 → 300)
- `batch_rows` (2000 → 5000)

### v2 and v3 delete-file counts are identical

Format-version toggle didn't take. Check:
- Iceberg version supports v3? (needs 1.5+)
- Spark config includes format-version support?
- Table property actually changed: `SHOW TBLPROPERTIES table_name;`

### Compaction is suspiciously fast

Check if `spark.sql.iceberg.auto-compact.enabled` is truly `false` in your Spark session. Auto-compaction running silently erases the accumulation you're trying to measure.

## References

- [Apache Iceberg Format Versions](https://iceberg.apache.org/spec/)
- [Iceberg Deletion Vectors](https://iceberg.apache.org/docs/latest/delete-vectors/)
- [Gunnar Morling's Hardwood Benchmarks](https://hardwood.openjdk.java.net/) — methodology inspiration

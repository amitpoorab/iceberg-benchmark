# Iceberg Benchmark Playground

A rigorous, peer-review-grade benchmark suite for Apache Iceberg that measures the compaction-cadence cost curve for late-arriving-data correction workloads, comparing **v2 positional deletes** against **v3 deletion vectors**.

## The Question

When late-arriving corrections scatter across historical partitions, **when should you compact?**

- Compact frequently (low cadence) → high compaction cost
- Compact rarely (high cadence) → high read degradation from accumulated deletes

Total cost = accumulated read degradation + compaction cost. **This forms a U-shaped curve.** We measure the minimum (C*) and test whether v3 deletion vectors shift or flatten it.

## Quick Start

```bash
# One-time setup (installs in development mode)
pip install -e .

# Run benchmark (5 min)
export BENCH_PROFILE=local
iceberg-benchmark --smoke

# View results
iceberg-plot
```

See [QUICKSTART.md](QUICKSTART.md) for details.

## Project Structure

```
iceberg-benchmark/
├── src/iceberg_benchmark/          # Main package
│   ├── __init__.py
│   ├── config.py                   # Configuration knobs
│   ├── spark_session.py            # Spark setup
│   ├── workload.py                 # Table & batch generation
│   ├── measure.py                  # Query latency & compaction
│   ├── sanity_check.py             # Pre-flight validation
│   ├── run_sweep.py                # Benchmark driver
│   └── plot.py                     # Visualization
├── tests/                          # Test suite
│   ├── test_setup.py               # Module validation
│   └── test_integration.py         # Spark integration test
├── pyproject.toml                  # Modern Python packaging
├── README.md                       # Full technical documentation
├── QUICKSTART.md                   # Quick start guide
├── PROJECT_README.md               # This file
├── GROUND_TRUTH.md                 # Prediction template
└── results/                        # Output directory
```

### Core Modules (7 modules)
- **config.py** — Single source of truth. `BENCH_PROFILE=local|ec2` flips scale only.
- **spark_session.py** — Spark 3.5 + Iceberg with **auto-compaction disabled** (manual only)
- **workload.py** — Iceberg table generation + late-arrival correction batches
- **measure.py** — Query latency, compaction cost, delete file tracking
- **sanity_check.py** — Pre-flight validation (fails if signal < 15%)
- **run_sweep.py** — Main driver. `--smoke` for quick validation, full sweep for production
- **plot.py** — Cost curve visualization

### CLI Entry Points
- `iceberg-benchmark` — Run the full benchmark sweep
- `iceberg-sanity-check` — Run pre-flight validation
- `iceberg-plot` — Visualize results

### Supporting Files
- **README.md** — Full technical documentation
- **QUICKSTART.md** — Fastest path to first results
- **GROUND_TRUTH.md** — Prediction template (fill before running)
- **pyproject.toml** — Modern Python packaging configuration

### Results
- **results/per_batch.csv** — Raw per-batch metrics (readable for peer review)
- **results/summary.csv** — Aggregated costs by (mode, cadence)
- **results/plot.gnuplot** — Publication-quality plot script

## Architecture

**Modular, no monolith:**

```
Local Smoke Test (5 min)          EC2 Full Benchmark (4-8 hrs)
├─ 100K rows × 12 batches        ├─ 200M rows × 168 batches
├─ 2 cadences × 1 mode           ├─ 4 cadences × 2 modes
└─ Validates pipeline             └─ Production results
```

Same code runs both ways. `BENCH_PROFILE=local` or `BENCH_PROFILE=ec2`.

## Measurement Discipline

This benchmark follows **Gunnar Morling's Hardwood methodology** for credibility:

✓ Warmup runs discarded (first 3 queries)  
✓ Median of measured runs (5 queries), not mean  
✓ Fixed pinned query set (full scan + 5% selective)  
✓ Deterministic seed (seed=42 for reproducibility)  
✓ Raw per-batch CSVs published (not just the plot)  
✓ Expected stability: ±3% across runs  

## Key Features

- **Peer-review ready** — raw data, ground truth predictions, reproducible seed
- **Scale agile** — 100K rows locally for dev, 200M rows on EC2 for production
- **Correct by construction** — manual compaction only (auto-compaction OFF)
- **v2 vs v3 comparison** — format-version toggle with delete-file tracking
- **Extensible** — modular design; easy to add new sweep parameters

## Run Modes

### Smoke Test (5 min, local)
Validates the pipeline doesn't break:
```bash
BENCH_PROFILE=local python run_sweep.py --smoke
```

### Sanity Check (EC2)
Pre-flight: ensure read degradation is measurable (≥15%):
```bash
BENCH_PROFILE=ec2 python sanity_check.py
```

### Full Sweep (1+ hours, EC2)
Production benchmark:
```bash
BENCH_PROFILE=ec2 python run_sweep.py
```

## Prerequisites

- **Python 3.10+** (already installed)
- **Java 17+** (already installed)
- **PySpark 3.5.0** (already installed)
- **iceberg-spark 1.11.0** (`pip install iceberg-spark==1.11.0`)

## Next Steps

1. Read [QUICKSTART.md](QUICKSTART.md)
2. Run `python run_sweep.py --smoke`
3. Check `results/summary.csv`
4. For EC2: fill [GROUND_TRUTH.md](GROUND_TRUTH.md), run full sweep, publish

## Why This Matters

Late-arriving data is **common in production:**
- Mobile events arriving out-of-order
- Batch corrections from upstream systems
- Data warehouse backfills

But choosing compaction cadence is **largely guess-and-check.** This benchmark **proves the cost curve** and **measures whether v3 deletion vectors actually help** (not just theoretically).

Results are suitable for:
- Blog posts / technical articles
- Iceberg documentation
- Production decision-making
- Conference talks

## Files & Structure

```
iceberg-benchmark/
├── config.py                 # Configuration knobs
├── spark_session.py          # Spark setup
├── workload.py               # Data generation
├── measure.py                # Measurement utilities
├── sanity_check.py           # Pre-flight validation
├── run_sweep.py              # Main driver
├── plot.py                   # Visualization
├── test_setup.py             # Module tests
├── test_integration.py       # Spark test
├── QUICKSTART.md             # ← Start here
├── README.md                 # Full docs
├── PROJECT_README.md         # This file
├── GROUND_TRUTH.md           # Predictions
├── requirements.txt          # Dependencies
├── .gitignore
└── results/                  # Output CSVs
    ├── per_batch.csv
    ├── summary.csv
    └── plot.gnuplot
```

## License

This benchmark is provided as-is for educational and research purposes.

## Questions?

See [README.md](README.md) for detailed documentation.
See [QUICKSTART.md](QUICKSTART.md) for fastest setup.

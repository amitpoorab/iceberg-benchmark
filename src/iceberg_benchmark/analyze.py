"""
Analysis script for research hypotheses validation.

Generates:
- H2: Cost curves (U-shape validation)
- H4: v2 vs v3 policy comparison
- H5: Elasticity analysis (maintenance robustness)
"""

import csv
import json
import sys
from pathlib import Path
from typing import List, Dict

from iceberg_benchmark.cost_model import (
    CostMetrics,
    aggregate_per_batch_csv,
    find_optimal_cadence,
    compute_elasticity,
    compare_v2_vs_v3,
)


def load_per_batch_csv(csv_path: Path) -> List[Dict]:
    """Load per_batch.csv and parse rows."""
    results = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


def analyze_hypothesis_2(metrics_list: List[CostMetrics]) -> None:
    """
    H2: Optimal maintenance point exists (U-shaped cost curve).

    For each workload × format_version, find the minimum cost.
    """
    print("\n" + "="*80)
    print("H2: Optimal Maintenance Point Exists (U-Shaped Curve)")
    print("="*80)

    # Group by format_version and workload
    grouped = {}
    for m in metrics_list:
        key = (m.format_version, m.workload_id)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(m)

    results_h2 = []
    for (fv, wid), metrics_for_group in sorted(grouped.items()):
        opt_cadence, opt_cost = find_optimal_cadence(metrics_list, fv, wid)

        print(f"\nFormat v{fv}, Workload {wid}:")
        print(f"  Optimal cadence: {opt_cadence}")
        print(f"  Optimal cost: {opt_cost:.0f} ms")
        print(f"  Cost range: {min(m.total_cost_ms for m in metrics_for_group):.0f} - {max(m.total_cost_ms for m in metrics_for_group):.0f} ms")

        # Check if U-shaped (cost increases on both sides of optimum)
        costs_by_cadence = sorted([(m.cadence, m.total_cost_ms) for m in metrics_for_group])
        left_of_opt = [c for cad, c in costs_by_cadence if cad < opt_cadence]
        right_of_opt = [c for cad, c in costs_by_cadence if cad > opt_cadence]

        is_u_shaped = (
            len(left_of_opt) > 0 and len(right_of_opt) > 0 and
            min(left_of_opt) > opt_cost and min(right_of_opt) > opt_cost
        )
        print(f"  U-shaped: {is_u_shaped}")

        results_h2.append({
            'format_version': fv,
            'workload_id': wid,
            'optimal_cadence': opt_cadence,
            'optimal_cost_ms': opt_cost,
            'is_u_shaped': is_u_shaped,
        })

    return results_h2


def analyze_hypothesis_4_5(metrics_list: List[CostMetrics]) -> None:
    """
    H4: v3 changes optimal policy (different cadence)
    H5: v3 reduces elasticity (more robust to suboptimal cadence)
    """
    print("\n" + "="*80)
    print("H4 & H5: v3 Policy Change & Elasticity")
    print("="*80)

    # Get unique workloads
    workloads = set(m.workload_id for m in metrics_list)

    results_h4_5 = []
    for workload_id in sorted(workloads):
        comparison = compare_v2_vs_v3(metrics_list, workload_id)

        if not comparison:
            continue

        print(f"\nWorkload: {workload_id}")
        print(f"  v2 optimal cadence: {comparison['v2_optimal_cadence']}")
        print(f"  v3 optimal cadence: {comparison['v3_optimal_cadence']}")
        print(f"  Cadence shift: {comparison['cadence_shift_pct']:.1f}%")
        print(f"\n  v2 flattness (elasticity): {comparison['v2_flattness']:.3f}")
        print(f"  v3 flattness (elasticity): {comparison['v3_flattness']:.3f}")
        print(f"  Elasticity reduction: {comparison['elasticity_reduction_pct']:.1f}%")
        print(f"\n  v2 optimal cost: {comparison['v2_optimal_cost_ms']:.0f} ms")
        print(f"  v3 optimal cost: {comparison['v3_optimal_cost_ms']:.0f} ms")
        print(f"  Cost improvement: {comparison['cost_improvement_pct']:.1f}%")

        # Verdict
        major_cadence_shift = abs(comparison['cadence_shift_pct']) > 10
        major_elasticity_reduction = comparison['elasticity_reduction_pct'] and comparison['elasticity_reduction_pct'] > 20

        print(f"\n  → Major cadence shift (>10%): {major_cadence_shift}")
        print(f"  → Major elasticity reduction (>20%): {major_elasticity_reduction}")

        results_h4_5.append(comparison)

    return results_h4_5


def main():
    results_dir = Path("results")
    per_batch_csv_path = results_dir / "per_batch.csv"

    if not per_batch_csv_path.exists():
        print(f"Error: {per_batch_csv_path} not found")
        print("Run benchmark first: python -m iceberg_benchmark.run_sweep")
        sys.exit(1)

    # Load data
    print("Loading per_batch.csv...")
    per_batch_data = load_per_batch_csv(per_batch_csv_path)
    print(f"Loaded {len(per_batch_data)} batch records")

    # Aggregate into cost metrics
    print("Aggregating costs...")
    metrics_list = []
    for row in per_batch_data:
        # Simple grouping: assume all rows in file are same workload
        # In full version, would extract workload_id from row
        # For now, aggregate all as one workload
        pass

    # For now, just load summary.csv if it exists
    summary_csv_path = results_dir / "summary.csv"
    if summary_csv_path.exists():
        print("\nLoading summary.csv...")
        with open(summary_csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                metrics = CostMetrics(
                    format_version=int(row.get("format_version", 2)),
                    cadence=int(row.get("cadence", 1)),
                    workload_id="default",
                    batches=int(row.get("batches", 0)),
                    total_compact_ms=float(row.get("total_compact_ms", 0)),
                    total_read_penalty_ms=float(row.get("total_read_penalty_ms", 0)),
                    total_cost_ms=float(row.get("total_cost_ms", 0)),
                    max_delete_files=int(row.get("max_delete_files", 0)),
                    avg_full_scan_ms=0.0,
                    avg_selective_ms=0.0,
                )
                metrics_list.append(metrics)

    if not metrics_list:
        print("No cost metrics found. Exiting.")
        sys.exit(1)

    # Analyze hypotheses
    h2_results = analyze_hypothesis_2(metrics_list)
    h4_5_results = analyze_hypothesis_4_5(metrics_list)

    # Save analysis results
    analysis_output = {
        'h2_results': h2_results,
        'h4_5_results': h4_5_results,
    }

    output_path = results_dir / "analysis.json"
    with open(output_path, "w") as f:
        json.dump(analysis_output, f, indent=2)

    print(f"\n\nAnalysis results saved to {output_path}")
    print("\n✓ Analysis complete")


if __name__ == "__main__":
    main()

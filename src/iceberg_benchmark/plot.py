"""
Plot benchmark results: cost curves for v2 vs v3 across cadences.
"""

import csv
from pathlib import Path
from typing import Dict, List
import sys


def load_summary(csv_path: Path) -> List[Dict]:
    """Load summary.csv results."""
    results = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row["cadence"] = int(row["cadence"]) if row["cadence"] != "inf" else float("inf")
            row["total_cost_ms"] = float(row["total_cost_ms"])
            row["total_read_penalty_ms"] = float(row["total_read_penalty_ms"])
            row["total_compact_ms"] = float(row["total_compact_ms"])
            results.append(row)
    return results


def find_optimal_cadence(results: List[Dict], mode: str) -> tuple:
    """Find optimal cadence (minimum cost) for a given delete mode."""
    mode_results = [r for r in results if r["mode"] == mode]
    if not mode_results:
        return None, None
    min_result = min(mode_results, key=lambda r: r["total_cost_ms"])
    return min_result["cadence"], min_result["total_cost_ms"]


def print_ascii_plot(results: List[Dict]) -> None:
    """Print ASCII plot of cost curves."""
    v2_results = [(r["cadence"], r["total_cost_ms"]) for r in results if r["mode"] == "v2"]
    v3_results = [(r["cadence"], r["total_cost_ms"]) for r in results if r["mode"] == "v3"]

    v2_results.sort(key=lambda x: x[0] if x[0] != float("inf") else 999999)
    v3_results.sort(key=lambda x: x[0] if x[0] != float("inf") else 999999)

    if not v2_results and not v3_results:
        print("No results to plot")
        return

    # Simple text output
    print("\n" + "="*70)
    print("BENCHMARK RESULTS: Compaction-Cadence Cost Curve")
    print("="*70)

    print("\nv2 (Positional Deletes):")
    for cadence, cost in v2_results:
        cadence_str = f"inf" if cadence == float("inf") else str(int(cadence))
        print(f"  Cadence {cadence_str:>4}: {cost:>10.0f}ms")

    v2_optimal_cadence, v2_optimal_cost = find_optimal_cadence(results, "v2")
    if v2_optimal_cadence:
        cadence_str = "inf" if v2_optimal_cadence == float("inf") else str(int(v2_optimal_cadence))
        print(f"  → Optimal C* at cadence {cadence_str}: {v2_optimal_cost:.0f}ms")

    print("\nv3 (Deletion Vectors):")
    for cadence, cost in v3_results:
        cadence_str = f"inf" if cadence == float("inf") else str(int(cadence))
        print(f"  Cadence {cadence_str:>4}: {cost:>10.0f}ms")

    v3_optimal_cadence, v3_optimal_cost = find_optimal_cadence(results, "v3")
    if v3_optimal_cadence:
        cadence_str = "inf" if v3_optimal_cadence == float("inf") else str(int(v3_optimal_cadence))
        print(f"  → Optimal C* at cadence {cadence_str}: {v3_optimal_cost:.0f}ms")

    # Comparison
    if v2_optimal_cost and v3_optimal_cost:
        improvement_pct = ((v2_optimal_cost - v3_optimal_cost) / v2_optimal_cost) * 100
        print(f"\nv3 improvement at optimal cadence: {improvement_pct:.1f}%")

    print("="*70 + "\n")


def generate_gnuplot_script(results: List[Dict], output_path: Path) -> None:
    """Generate a gnuplot script for publication-quality plot."""
    plot_script = output_path / "plot.gnuplot"

    v2_results = [(r["cadence"], r["total_cost_ms"]) for r in results if r["mode"] == "v2"]
    v3_results = [(r["cadence"], r["total_cost_ms"]) for r in results if r["mode"] == "v3"]

    v2_results.sort(key=lambda x: x[0] if x[0] != float("inf") else 999999)
    v3_results.sort(key=lambda x: x[0] if x[0] != float("inf") else 999999)

    v2_optimal_cadence, v2_optimal_cost = find_optimal_cadence(results, "v2")
    v3_optimal_cadence, v3_optimal_cost = find_optimal_cadence(results, "v3")

    with open(plot_script, "w") as f:
        f.write("""set terminal pngcairo size 1200,600 font "Helvetica,12"
set output "cost_curve.png"
set title "Iceberg Compaction-Cadence Cost Curve: v2 Positional Deletes vs v3 Deletion Vectors"
set xlabel "Compaction Cadence (batches)"
set ylabel "Total Cost (ms) = Read Penalty + Compaction"
set grid
set key outside right

# Plot data
plot \\
""")

        # Add v2 data
        if v2_results:
            f.write('    "-" using 1:2 with linespoints title "v2 (Positional Deletes)" linewidth 2 pointsize 1.5, \\\n')

        # Add v3 data
        if v3_results:
            f.write('    "-" using 1:2 with linespoints title "v3 (Deletion Vectors)" linewidth 2 pointsize 1.5\n')

        # v2 data
        if v2_results:
            f.write("\n")
            for cadence, cost in v2_results:
                cadence_val = cadence if cadence != float("inf") else 999
                f.write(f"{cadence_val} {cost}\n")
            f.write("e\n")

        # v3 data
        if v3_results:
            f.write("\n")
            for cadence, cost in v3_results:
                cadence_val = cadence if cadence != float("inf") else 999
                f.write(f"{cadence_val} {cost}\n")
            f.write("e\n")

    print(f"Gnuplot script written to {plot_script}")
    print("To generate plot: gnuplot plot.gnuplot")


def main():
    results_dir = Path("results")
    summary_csv = results_dir / "summary.csv"

    if not summary_csv.exists():
        print(f"Error: {summary_csv} not found")
        print("Run: python -m iceberg_benchmark.run_sweep")
        sys.exit(1)

    results = load_summary(summary_csv)

    if not results:
        print("No results to plot")
        sys.exit(1)

    # Print ASCII plot
    print_ascii_plot(results)

    # Generate gnuplot script
    generate_gnuplot_script(results, results_dir)

    print("Results summary:")
    for r in results:
        print(f"  {r['mode']} cadence {r['cadence']}: {r['total_cost_ms']:.0f}ms")


if __name__ == "__main__":
    main()

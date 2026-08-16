"""
Cost model for Iceberg maintenance optimization.

Computes total cost as: C_total(t) = C_maintenance(t) + C_read_penalty(t)

where:
  C_maintenance = compaction cost (wall-clock time, compute)
  C_read_penalty = accumulated latency penalty from deletes above baseline

Models the U-shaped curve and derives optimal cadence.
"""

from typing import Dict, List, Tuple
from dataclasses import dataclass
import json


@dataclass
class CostMetrics:
    """Aggregated cost metrics for a (format_version, cadence, workload) cell."""
    format_version: int
    cadence: int
    workload_id: str
    batches: int

    # Costs (in milliseconds)
    total_compact_ms: float      # Sum of all compaction times
    total_read_penalty_ms: float # Sum of query latencies above baseline
    total_cost_ms: float         # total_compact_ms + total_read_penalty_ms

    # Metadata
    max_delete_files: int
    avg_full_scan_ms: float
    avg_selective_ms: float


def aggregate_per_batch_csv(per_batch_csv: List[Dict]) -> Tuple[str, CostMetrics]:
    """
    Aggregate per-batch results into cost metrics.

    Args:
        per_batch_csv: List of dicts from per_batch.csv

    Returns:
        (workload_key, CostMetrics)
    """
    if not per_batch_csv:
        return None, None

    # Extract first row for metadata
    first = per_batch_csv[0]
    format_version = first.get("format_version")
    cadence = first.get("cadence")
    workload_id = first.get("workload_id", "default")

    # Aggregate costs
    total_compact_ms = sum(float(row.get("compact_duration_ms", 0)) for row in per_batch_csv)

    # Read penalty = sum of latencies above baseline (first batch, assumed zero penalty)
    baseline_latency = float(per_batch_csv[0].get("full_scan_ms", 0))
    total_read_penalty_ms = sum(
        max(0, float(row.get("full_scan_ms", 0)) - baseline_latency)
        for row in per_batch_csv
    )

    total_cost_ms = total_compact_ms + total_read_penalty_ms
    max_delete_files = max(int(row.get("delete_files", 0)) for row in per_batch_csv)
    avg_full_scan_ms = sum(float(row.get("full_scan_ms", 0)) for row in per_batch_csv) / len(per_batch_csv)
    avg_selective_ms = sum(float(row.get("selective_ms", 0)) for row in per_batch_csv) / len(per_batch_csv)

    metrics = CostMetrics(
        format_version=format_version,
        cadence=cadence,
        workload_id=workload_id,
        batches=len(per_batch_csv),
        total_compact_ms=total_compact_ms,
        total_read_penalty_ms=total_read_penalty_ms,
        total_cost_ms=total_cost_ms,
        max_delete_files=max_delete_files,
        avg_full_scan_ms=avg_full_scan_ms,
        avg_selective_ms=avg_selective_ms,
    )

    workload_key = f"{format_version}_{cadence}_{workload_id}"
    return workload_key, metrics


def find_optimal_cadence(
    metrics_list: List[CostMetrics],
    format_version: int,
    workload_id: str,
) -> Tuple[int, float]:
    """
    Find optimal cadence (minimum cost) for a given format version and workload.

    Args:
        metrics_list: List of CostMetrics
        format_version: 2 or 3
        workload_id: Workload identifier

    Returns:
        (optimal_cadence, optimal_cost_ms)
    """
    relevant = [
        m for m in metrics_list
        if m.format_version == format_version and m.workload_id == workload_id
    ]

    if not relevant:
        return None, None

    optimal = min(relevant, key=lambda m: m.total_cost_ms)
    return optimal.cadence, optimal.total_cost_ms


def compute_elasticity(
    metrics_list: List[CostMetrics],
    format_version: int,
    workload_id: str,
) -> Dict:
    """
    Compute maintenance elasticity: sensitivity of cost to cadence deviation.

    Elasticity = ΔCost / ΔCadence

    Lower elasticity = less sensitive to suboptimal cadence (better for ops)

    Args:
        metrics_list: List of CostMetrics
        format_version: 2 or 3
        workload_id: Workload identifier

    Returns:
        {
            'optimal_cadence': int,
            'optimal_cost': float,
            'elasticity_at_1_5x': float,  # Cost increase at 1.5x optimal cadence
            'elasticity_at_0_5x': float,  # Cost increase at 0.5x optimal cadence
            'flattness': float,           # Lower = flatter curve (more robust)
        }
    """
    opt_cadence, opt_cost = find_optimal_cadence(metrics_list, format_version, workload_id)

    if opt_cadence is None:
        return None

    # Find costs at nearby cadences
    relevant = [
        m for m in metrics_list
        if m.format_version == format_version and m.workload_id == workload_id
    ]

    # Cost at 1.5x optimal (if available)
    cost_1_5x = None
    cadence_1_5x = opt_cadence * 1.5
    closest_1_5x = min(
        (m for m in relevant if m.cadence >= cadence_1_5x),
        key=lambda m: abs(m.cadence - cadence_1_5x),
        default=None
    )
    if closest_1_5x:
        cost_1_5x = closest_1_5x.total_cost_ms

    # Cost at 0.5x optimal
    cost_0_5x = None
    cadence_0_5x = opt_cadence * 0.5
    closest_0_5x = min(
        (m for m in relevant if m.cadence <= cadence_0_5x),
        key=lambda m: abs(m.cadence - cadence_0_5x),
        default=None
    )
    if closest_0_5x:
        cost_0_5x = closest_0_5x.total_cost_ms

    # Elasticity at 1.5x
    elasticity_1_5x = ((cost_1_5x - opt_cost) / opt_cost) if cost_1_5x else None

    # Elasticity at 0.5x
    elasticity_0_5x = ((cost_0_5x - opt_cost) / opt_cost) if cost_0_5x else None

    # Flattness = average elasticity (lower = flatter = more robust)
    elasticities = [e for e in [elasticity_1_5x, elasticity_0_5x] if e is not None]
    flattness = sum(elasticities) / len(elasticities) if elasticities else None

    return {
        'optimal_cadence': opt_cadence,
        'optimal_cost_ms': opt_cost,
        'elasticity_at_1_5x': elasticity_1_5x,
        'elasticity_at_0_5x': elasticity_0_5x,
        'flattness': flattness,
    }


def compare_v2_vs_v3(
    metrics_list: List[CostMetrics],
    workload_id: str,
) -> Dict:
    """
    Compare v2 and v3 policies for a workload.

    Tests hypotheses H4 (policy change) and H5 (elasticity difference).

    Args:
        metrics_list: List of CostMetrics
        workload_id: Workload identifier

    Returns:
        {
            'v2_optimal_cadence': int,
            'v3_optimal_cadence': int,
            'cadence_shift_pct': float,           # % change in optimal cadence
            'v2_elasticity': float,
            'v3_elasticity': float,
            'elasticity_reduction_pct': float,   # % reduction v3 vs v2
            'v2_cost': float,
            'v3_cost': float,
            'cost_improvement_pct': float,
        }
    """
    elastic_v2 = compute_elasticity(metrics_list, 2, workload_id)
    elastic_v3 = compute_elasticity(metrics_list, 3, workload_id)

    if not elastic_v2 or not elastic_v3:
        return None

    cadence_shift = (
        (elastic_v3['optimal_cadence'] - elastic_v2['optimal_cadence'])
        / elastic_v2['optimal_cadence'] * 100
    )

    elasticity_reduction = None
    if elastic_v2['flattness'] and elastic_v3['flattness']:
        elasticity_reduction = (
            (elastic_v2['flattness'] - elastic_v3['flattness'])
            / elastic_v2['flattness'] * 100
        )

    cost_improvement = (
        (elastic_v2['optimal_cost_ms'] - elastic_v3['optimal_cost_ms'])
        / elastic_v2['optimal_cost_ms'] * 100
    )

    return {
        'v2_optimal_cadence': elastic_v2['optimal_cadence'],
        'v3_optimal_cadence': elastic_v3['optimal_cadence'],
        'cadence_shift_pct': cadence_shift,
        'v2_flattness': elastic_v2['flattness'],
        'v3_flattness': elastic_v3['flattness'],
        'elasticity_reduction_pct': elasticity_reduction,
        'v2_optimal_cost_ms': elastic_v2['optimal_cost_ms'],
        'v3_optimal_cost_ms': elastic_v3['optimal_cost_ms'],
        'cost_improvement_pct': cost_improvement,
    }

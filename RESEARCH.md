# From Fixed Schedules to Workload-Aware Maintenance: Optimizing Apache Iceberg Compaction Under Row-Level Updates

## Research Motivation

Apache Iceberg Issue #11122 identifies a critical tradeoff in v3 deletion vectors: fewer delete files but potential write/read overhead. However, the design question remains operational:

**Given Iceberg's deletion mechanisms, when should an operator actually compact?**

Existing systems (Gravitino, AWS, Dremio) use heuristic policies (e.g., `compact if delete_file_count > 100`), but these are not economically optimized. This research investigates whether workload characteristics can predict the optimal maintenance policy, and how Iceberg v3 changes that policy space.

---

## Research Questions

### Primary
**RQ1:** Can we derive a workload-dependent maintenance policy that minimizes combined compaction and read-penalty costs?

**RQ2:** How does the optimal maintenance interval change between Iceberg v2 and v3?

### Secondary
**RQ3:** How sensitive is total workload cost to suboptimal (operator-chosen) maintenance cadence?

**RQ4:** Are existing heuristic policies (e.g., delete-file-count thresholds) economically sound?

---

## Hypotheses

### H1: Baseline — Read Degradation is Measurable
**H1:** Increasing unresolved row-level deletes produces measurable read-performance degradation, with the relationship depending on workload characteristics (update rate, read selectivity, data file size).

**Rationale:** Establishes that delete accumulation matters operationally.

**Expectation:** Full-table scan latency increases monotonically with delete-file count; selective queries are more affected than full scans.

---

### H2: Optimal Maintenance Point Exists
**H2:** For a fixed workload, there exists a measurable maintenance interval t* at which the marginal cost of additional compaction is lower than the accumulated cost of deferred delete processing.

**Rationale:** Compaction has cost (compute, I/O), but so does accumulated deletes. An economic optimum should exist.

**Mathematical formulation:**
```
C_total(t) = C_maintenance(t) + C_read_penalty(t)

where:
  C_maintenance(t) = compaction cost accumulated over interval [0, t]
  C_read_penalty(t) = query latency penalty from accumulated deletes over [0, t]

Objective: t* = argmin_t C_total(t)
```

**Expectation:** Cost curves exhibit U-shape; v2 and v3 may have different minima.

---

### H3: Workload Characteristics Predict Optimal Policy
**H3:** The optimal maintenance interval t* is predictable from workload characteristics: update rate, read rate, delete fraction, average data-file size, and query selectivity.

**Rationale:** If true, operators can derive maintenance policy from observable workload metrics without trial-and-error.

**Functional form (testable):**
```
t* = f(update_rate, read_rate, delete_fraction, file_size, selectivity)
```

**Expectation:** Different workloads yield different t*; policies should adapt rather than use fixed schedules.

---

### H4: Iceberg v3 Materially Changes the Policy Space
**H4:** Iceberg v3 deletion vectors change the workload-to-maintenance relationship materially, allowing longer maintenance intervals and/or reducing the performance penalty of delayed maintenance relative to v2 under equivalent workloads.

**Careful wording:** We do not assume v3 is always better; we measure whether the policy changes.

**Testable claims:**
- v3 optimal cadence (C*_v3) differs from v2 (C*_v2) for the same workload
- v3 exhibits lower cost penalty for operating at suboptimal cadence (maintenance elasticity)

---

### H5: Maintenance Elasticity Differs Between v2 and v3
**H5:** The sensitivity of total cost to suboptimal cadence (maintenance elasticity) is lower under v3 than v2.

**Mathematical definition:**
```
Elasticity(cadence) = ΔCost / ΔCadence

If E_v3 < E_v2, then v3 makes maintenance scheduling less sensitive to operator error.
```

**Visual intuition:**
```
v2: Sharp U-curve          v3: Shallow U-curve
Cost ^                     Cost ^
     |\                         |\
     | \____                     |  \________
     |      \____                |
  +------cadence>            +------cadence>

Operation at C_actual = 1.5 * C*:
  - v2: significant cost penalty
  - v3: smaller cost penalty
```

**Expectation:** v3 may not just improve absolute performance; it may make operations more forgiving.

---

## Experimental Design

### Workload Dimensions

We vary workload along four independent axes:

| Dimension | Low | Medium | High |
|-----------|-----|--------|------|
| **Update Rate** | 1% | 5% | 20% |
| **Delete Fraction** | 0.1% | 1% | 5% |
| **Read Rate** | 100 qps | 500 qps | 2000 qps |
| **Query Selectivity** | 1% | 5% | 50% |

**Design:** Latin hypercube or factorial subset (not cartesian product, cost-prohibitive).

### Measurement

For each (workload, format_version, cadence) cell:

**Cost metrics:**
- `C_maintenance`: Wall-clock time for compaction, amortized over batch interval
- `C_read_penalty`: Sum of query latencies above baseline, per batch interval
- `C_total = C_maintenance + C_read_penalty`

**Workload metrics:**
- `update_rate`: Rows modified per batch
- `delete_fraction`: Percentage of rows that are deletes
- `read_rate`: Queries per second
- `selectivity`: Percentage of rows matched by WHERE clause

**Delete metrics:**
- `delete_file_count`: Accumulated delete files
- `delete_byte_size`: Total bytes in delete files

### Outputs

**Per-batch CSV:**
```
batch_idx, format_version, cadence, workload_id,
update_rate, delete_fraction, read_rate, selectivity,
query_latency_ms, delete_file_count, compact_ms, total_cost_ms
```

**Summary CSV:**
```
format_version, cadence, workload_id,
update_rate, delete_fraction, read_rate, selectivity,
C_maintenance_ms, C_read_penalty_ms, C_total_ms,
elasticity, optimal_flag
```

---

## Analysis Plan

### Analysis 1: Cost Surface (Tests H2)

For each (format_version, workload), plot cost as a function of cadence:

```python
for workload in workloads:
  for fv in [v2, v3]:
    C_total = [cost(fv, cadence, workload) for cadence in cadences]
    plot(cadences, C_total)
    C_star[fv, workload] = argmin(C_total)
```

**Verdict:** U-shape visible? Minimum identifiable?

### Analysis 2: Workload-to-Policy Mapping (Tests H3)

Fit model predicting C* from workload characteristics:

```python
# Simple linear regression (iterate to nonlinear if needed)
C_star ~ β0 + β1*update_rate + β2*delete_frac + β3*read_rate + β4*selectivity

# Goodness of fit: R², cross-validation error
```

**Verdict:** Are workload features predictive (R² > 0.8)? Which features dominate?

### Analysis 3: v2 vs v3 Policy Comparison (Tests H4, H5)

```python
# For each workload:
policy_delta = C_star_v3 - C_star_v2
elasticity_v2 = (C_total(1.5*C_star) - C_total(C_star)) / (0.5*C_star)
elasticity_v3 = (C_total(1.5*C_star) - C_total(C_star)) / (0.5*C_star)
elasticity_ratio = elasticity_v3 / elasticity_v2

# Result: tables and plots
print(f"v3 shifts optimal cadence by {policy_delta:.1f}%")
print(f"v3 reduces elasticity by {(1 - elasticity_ratio)*100:.1f}%")
```

**Verdict:** Does v3 materially change policy? Is it flatter (lower elasticity)?

---

## Expected Contributions

### If H1-H3 Confirmed
- Empirical characterization of compaction-cadence cost tradeoff for row-level updates
- Predictive model relating workload to optimal maintenance policy
- Operational guidance for Iceberg users

### If H4-H5 Confirmed
- Quantitative evidence that v3 deletion vectors change operational dynamics (not just performance)
- Argument for v3 adoption beyond latency: "makes operations more robust"

### If H3 Strongly Confirmed
- Foundation for automated maintenance tools: `maintenance_policy = derive_from_workload(workload_metrics)`

---

## Limitations & Scope

- **Single machine execution** (Mac Pro / t3.2xlarge): No distributed compaction overhead
- **Synthetic workload** (CDC/row-level updates): Not testing all Iceberg use cases
- **Fixed batch sizes**: Does not explore time-based scheduling
- **No concurrent workloads**: Isolated read/write, not contention effects
- **No multi-table scenarios**: Single table focus

---

## Reproducibility

- **Code:** GitHub (iceberg-maintenance-policy)
- **Docker:** Reproducible on any Linux/Mac with 8GB+ RAM
- **Data:** All CSV results published (peer-reviewable)
- **Configuration:** All hyperparameters in config.py with explicit documentation
- **Randomness:** Fixed seed (seed=42) for deterministic results

---

## References

- Apache Iceberg Issue #11122: Improve Position Deletes in V3
- Iceberg Deletion Vectors: https://iceberg.apache.org/docs/latest/delete-vectors/
- Gravitino Compaction Policy: https://gravitino.apache.org/
- Related work on database maintenance scheduling: [TBD - literature review]

---

## Timeline

- **Phase 1 (Weeks 1-2):** Implement cost model, run H1-H2 validation
- **Phase 2 (Weeks 3-4):** Full workload sweep, analyze H3
- **Phase 3 (Weeks 5-6):** v2 vs v3 comparison, elasticity analysis (H4-H5)
- **Phase 4 (Weeks 7-8):** Write paper, create presentation, open-source tool

---

## Success Criteria

- ✅ H1: Read degradation curves are statistically significant (p < 0.05)
- ✅ H2: U-shaped cost curves are visible for ≥80% of workloads
- ✅ H3: Workload-to-policy model achieves R² ≥ 0.75 on holdout test set
- ✅ H4: v3 shows materially different optimal cadence (>10% shift) for ≥50% of workloads
- ✅ H5: v3 elasticity <80% of v2 elasticity for ≥70% of workloads
- ✅ Reproducibility: Any user can run `docker compose run` and replicate results within ±5% variance

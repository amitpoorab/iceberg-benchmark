# Ground Truth Predictions

**Purpose:** Commit your predicted optimal cadences and costs BEFORE running the benchmark.
This lets results be scored right/wrong rather than just observed.

## Before Running the Sweep

Fill in your predictions for both v2 (positional deletes) and v3 (deletion vectors):

### v2 Positional Deletes

| Metric | Value | Notes |
|--------|-------|-------|
| Optimal Cadence (C*) | — batches | Your prediction for cadence that minimizes total cost |
| Predicted Total Cost at C* | — ms | Sum of read penalty + compaction cost |
| Read Penalty Component | — ms | Accumulated read degradation |
| Compaction Cost Component | — ms | Total compaction time across all batches |

**Reasoning:**

[Explain your hypothesis. Example: "Compaction cost dominates at low cadences (e.g., cadence=1 means compact after every batch). Read penalty dominates at high cadences (no compaction). I expect the curve to bottom out at cadence=24 because my workload has ~20-30 day windows of late-arriving corrections."]

---

### v3 Deletion Vectors

| Metric | Value | Notes |
|--------|-------|-------|
| Optimal Cadence (C*) | — batches | Your prediction |
| Predicted Total Cost at C* | — ms | Your prediction |
| Read Penalty Component | — ms | Your prediction |
| Compaction Cost Component | — ms | Your prediction |

**Reasoning:**

[Explain how deletion vectors differ from positional deletes. Example: "Deletion vectors should shift C* higher because they're cheaper to compact (no rewrite). I predict cadence=48, with 20% lower compaction cost."]

---

## After Running the Sweep

Once the benchmark completes, fill in actual results and score your predictions:

### v2 Actual Results

| Metric | Actual | Predicted | Match? |
|--------|--------|-----------|--------|
| Optimal Cadence | — | — | ✓ or ✗ |
| Cost at C* | — ms | — ms | ±% |
| Read Penalty | — ms | — ms | ±% |
| Compaction Cost | — ms | — ms | ±% |

### v3 Actual Results

| Metric | Actual | Predicted | Match? |
|--------|--------|-----------|--------|
| Optimal Cadence | — | — | ✓ or ✗ |
| Cost at C* | — ms | — ms | ±% |
| Read Penalty | — ms | — ms | ±% |
| Compaction Cost | — ms | — ms | ±% |

### Comparison: v3 vs v2

| Aspect | Result | Notes |
|--------|--------|-------|
| Cost improvement at v3 C* | — % | (v2_cost - v3_cost) / v2_cost |
| Cadence shift | — | Did C* move? If so, by how much? |
| Curve flatness | — | Is v3 curve flatter (more forgiving of cadence choice)? |

---

## Notes on Predictions

- Be honest about uncertainty. If you don't know, write "unknown" rather than guessing.
- Base predictions on your knowledge of the workload (batch size, partition scatter, compaction cost model).
- After running, compare actual to predicted. If they diverge significantly, investigate:
  - Was your mental model of compaction cost wrong?
  - Did the workload differ from expected (e.g., fewer scattered partitions)?
  - Is there a bug in measurement or configuration?

---

## Scorecard

Once actual results are in, score yourself:

```
Prediction accuracy:
  v2 C*: [correct / off-by-N / wrong]
  v3 C*: [correct / off-by-N / wrong]
  Cost estimates: [±5% / ±10% / ±20% / way off]

What you learned:
  - [Unexpected finding]
  - [Hypothesis confirmed/refuted]
  - [Parameter to adjust next time]
```

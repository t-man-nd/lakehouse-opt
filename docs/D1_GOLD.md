# D1 — Gold aggregation

E1 builds Gold from the Silver snapshot **after C1 and C2**. The default
`zone_hourofday` grain follows the assignment: `PULocationID` × hour of day
(0–23), pooling that hour across dates. Spark's session timezone is pinned to
UTC by the API and CLI.

```bash
python -m src.gold --silver-dir data/e1_runs/<run>/silver \
  --gold-dir data/e1_runs/<run>/gold_rebuild \
  --grain zone_hourofday --output docs/d1_run.json
```

The complete command is documented in [E1_RUNBOOK.md](E1_RUNBOOK.md). The latest
accepted counts and snapshot versions come from [pipeline_run.json](pipeline_run.json).
The older 2,880-row B3 baseline and 2,983-row post-C2 snapshot are different inputs;
never compare their Gold bucket totals as if they were the same run.

## Metric definitions

| Column | Definition |
|---|---|
| `trip_count` | Number of trips in the bucket |
| `avg_fare` | `AVG(fare_amount)` |
| `avg_tip_pct` | `100 * AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END)` |
| `driver_earnings` | `SUM(COALESCE(fare_amount,0) + COALESCE(tip_amount,0) + COALESCE(extra,0))` |
| `avg_distance` | `AVG(trip_distance)` |
| `total_revenue` | `SUM(total_amount)` |
| `avg_passengers` | `AVG(passenger_count)` |

Tip percentage is an average of per-trip ratios. For fares 10 and 20 with tips
2 and 2, the result is 15%, while the ratio of sums is 13.3333%. Null ratios are
excluded by SQL `AVG`; zero/negative fares produce a null ratio, avoiding ANSI
division errors. Null components of the assignment-defined earnings metric are
coalesced to zero. This metric is gross fare + tip + extra, not net take-home pay;
operating expenses are not in the data.

`total_amount` is used directly for revenue. Any discrepancy between that field
and a sum of individual receipt components requires separate investigation. The
previous claim that an omitted `$0.75` CBD fee explained the discrepancy was
unsupported and has been removed.

## Verification and output

`verify()` constructs an independent SQL aggregation from the original Silver
columns and compares every Gold bucket, every count, and every reported metric.
It rejects missing, extra, duplicate or numerically different buckets and checks
that `SUM(trip_count)` equals the Silver count. Empty input and null tip ratios
are supported; the summary retains SQL nulls where an average is undefined.

The JSON summary records the pinned source Silver version, UTC timezone, grain,
counts, independent SQL verification, metric definitions and sample profile.
`tests/test_gold.py` covers the per-trip definition, earnings, denominator guard,
nulls, empty input, UTC bucketing, changed metrics, and a Delta write/read rebuild.

The E1 `gold.run()` API refuses an existing output unless `overwrite=True` is
explicitly supplied. `gold.build()` and the standalone rebuilding CLI deliberately
rebuild derived Gold. Silver and Gold paths cannot overlap.

## Optional grains and interpretation

| Grain | Keys | Use |
|---|---|---|
| `zone_hourofday` | Zone × hour of day | Assignment and E1 default |
| `zone_hour` | Zone × calendar hour | Detailed dated buckets |
| `zone_date` | Zone × calendar date | Coarser daily view |

B1 selects the first eligible records in each source file, not a random citywide
sample. The resulting sample is concentrated around three month boundaries.
`profile()` records the actual dates, hour distribution, bucket sizes and
singleton share. Interpret results only for these sampled trips; do not infer
representative NYC demand or population tipping behavior.

# B1 supplementary artifacts

Derived from B1's delivered files. **The generator was not re-run and the
ground truth is unchanged** — these are additional views over the same data,
so B1's verification still stands.

| File | For | Why it exists |
|---|---|---|
| `bronze_schema.json` | **B2** | Explicit Spark `StructType`. Reading the three batches file-by-file makes Spark infer a schema *per file*; a column can come back with different types and the union then fails or silently nulls it. Passing this schema removes the problem. `tpep_*` are **StringType on purpose** — malformed dates must survive Bronze. |
| `defect_index.json` | **B3, C1, E2** | Which `trip_id` carries which defect, plus three ready-made pools. Counts alone prove nothing was lost; ids prove the *right* rows were rejected. |
| `fixture_mini.json` | **B3, E2** | 18 rows, all four defect classes, disjoint. Unit-test the rules in under a second instead of loading 3,060 rows. |
| `fixture_mini_expected.json` | **B3, E2** | Ground truth for the fixture: 8 silver + 10 rejected = 18. |
| `checksums.txt` | everyone | sha256 of every B1 file. Confirms the whole team holds identical inputs. |

## Pools in `defect_index.json`

| Pool | Size | Use |
|---|---|---|
| `survives_to_silver` | **2880** | Every id that reaches Silver. Independently derived — matches `expected_silver_rows` exactly, a second confirmation of B1's manifest. |
| `clean_and_unique` | **2820** | Survives Silver **and** is not duplicated. **Sample C1's MERGE fixture from here.** |
| `duplicated_but_clean` | **60** | The duplicated ids. Otherwise valid; only the excess copy is rejected. |

## Why C1 must sample from `clean_and_unique`

If C1 draws update `trip_id`s from raw Bronze, it can pick a row Silver
rejected. That id then does not exist in Silver, the MERGE falls to the
**unmatched** branch, and a row is *inserted* instead of *updated*. The
statement succeeds, the count rises, the log looks fine — and
`whenMatchedUpdateAll` never fired. Sampling from `clean_and_unique` makes that
impossible.

## Usage

**B2 — explicit schema:**

```python
import json
from pyspark.sql.types import StructType

schema = StructType.fromJson(json.load(open("b1/bronze_schema.json")))
df = spark.read.schema(schema).json("b1/batch_*.json")
assert df.count() == 3060
```

**B3 — assert the right rows, not just the right count:**

```python
idx = json.load(open("b1/defect_index.json"))
expected_bad_dates = set(idx["batches"]["batch_01"]["defect_ids"]["INVALID_PICKUP_DATETIME"])

actual = {r.trip_id for r in rejected
          .filter("reject_reason = 'INVALID_PICKUP_DATETIME' AND _batch_id = 'batch_01'")
          .select("trip_id").collect()}

assert actual == expected_bad_dates
```

Reconciling totals proves no rows vanished. Comparing id **sets** proves the
predicates are correct — reject 30 valid rows and miss 30 invalid ones and the
totals still balance perfectly.

**C1 — safe fixture:**

```python
pool = json.load(open("b1/defect_index.json"))["pools"]["clean_and_unique"]
import random; random.seed(42)
update_ids = random.sample(pool, 500)   # guaranteed to exist in Silver
```

**B3/E2 — fast unit test:**

```python
df  = spark.read.schema(schema).json("b1/fixture_mini.json")
exp = json.load(open("b1/fixture_mini_expected.json"))
assert df.count() == exp["raw_rows"]          # 18
# after cleaning: 8 silver + 10 rejected
```

## Verify your copy

```bash
cd b1 && sha256sum -c checksums.txt
```

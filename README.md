# Alethe

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.21193962.svg)](https://doi.org/10.5281/zenodo.21193962)

Reference implementation of the **Observability Watermark Specification (OWS v0.1.0-draft)**. From the Greek *aletheia* — truth as un-forgetting.

## The problem

Every modern lakehouse supports time travel — and silently breaks it. The moment `VACUUM` (Delta) or a lifecycle policy (Iceberg) runs, the transaction log still *lists* historical versions that are physically unreadable. A point-in-time query against them either fails at runtime with no plan-time warning, or — worse — returns an empty result that looks like a real zero.

No current system distinguishes **"this fact was absent at time T"** from **"the evidence needed to answer this question was destroyed."** Alethe makes that boundary legible, provable, and enforceable:

```python
import alethe

wm = alethe.watermark("path/to/delta/table")   # empirically validated, not metadata arithmetic
alethe.record(wm, "watermarks.jsonl")           # hash-chained, tamper-evident ledger
alethe.verdict(wm, since=some_datetime)         # EXACT / BOUNDED — or an honest refusal
```

The thesis: **unknowability should be a value, not an error.** That makes *finite* retention safe to rely on — it is not an argument for infinite retention.

## Abstract (from the spec)

Modern data platforms support point-in-time (PIT) queries over historical
state, but silently lose the ability to answer them as retention policies
destroy history. This specification defines the **observability watermark**: a
provable, per-chain statement of how far back in time a data asset can be
honestly queried, together with the evidence that justifies the claim. It
further defines how watermarks compose across lineage, how queries over
watermarked assets produce **verdicts** (exact, bounded, or refused), and
the integrity requirements that make those verdicts usable as audit
evidence.

The unit of adoption is the specification ([`markdown/ows-spec-draft.md`](markdown/ows-spec-draft.md)), not any implementation. Alethe exists to prove the spec is implementable across real, heterogeneous infrastructure.

## The observability semiring

Query semantics rest on a three-valued semiring `K = {ABSENT, BEYOND, OBSERVED}`:

```
OBSERVED  ─── fact is present and inside retention  (multiplicative identity)
   │
BEYOND    ─── query exceeds the retention boundary; honest refusal
   │
ABSENT    ─── fact is definitively not present      (additive identity)
```

| ⊕ `(union)` | **ABSENT** | **BEYOND** | **OBSERVED** |   | ⊗ `(join)` | **ABSENT** | **BEYOND** | **OBSERVED** |
|---|---|---|---|---|---|---|---|---|
| **ABSENT**   | ABSENT  | BEYOND   | OBSERVED |   | **ABSENT**   | ABSENT | ABSENT | ABSENT   |
| **BEYOND**   | BEYOND  | BEYOND   | OBSERVED |   | **BEYOND**   | ABSENT | BEYOND | BEYOND   |
| **OBSERVED** | OBSERVED| OBSERVED | OBSERVED |   | **OBSERVED** | ABSENT | BEYOND | OBSERVED |

`⊕` is max — if any derivation path is OBSERVED, the row is observed. `⊗` is min — a join is only as knowable as its least knowable conjunct. `BEYOND` taint enters at a vacuumed source and propagates through every downstream join and projection **by algebra alone** — zero special-case logic in operators. All 126 semiring laws are verified exhaustively at import (`alethe.verify_semiring_laws()`).

## Architecture

```
  PHYSICAL TABLES                 ALETHE                        CONSUMERS
┌──────────────────┐   ┌───────────────────────────┐   ┌─────────────────────────┐
│  Delta Lake       │   │  adapters/delta            │   │  dbt                     │
│  (_delta_log,     ├──►│    replay log + empirical  │   │   DbtLineage: DAG walk,  │
│   VACUUM)         │   │    time-travel validation  │   │   pit_report, twice-     │
├──────────────────┤   ├───────────────────────────┤   │   temporal check         │
│  Apache Iceberg   │   │  adapters/iceberg          │   │   rewrite_model: zone-   │
│  (snapshots,      ├──►│    suffix boundary +       │──►│   gated TIMESTAMP AS OF  │
│   orphan cleanup) │   │    readable islands        │   │   / SCD2 predicates      │
└──────────────────┘   ├───────────────────────────┤   ├─────────────────────────┤
                        │  Watermark                 │   │  Airflow / OpenLineage   │
                        │   {boundary, evidence      │   │   to_run_event() emits   │
                        │    grade, proof,           │──►│   observabilityWatermark │
                        │    claim_recorded_at}      │   │   facets to Marquez etc. │
                        ├───────────────────────────┤   ├─────────────────────────┤
                        │  Manifest (JSONL ledger)   │   │  CI                      │
                        │   hash-chained, append-    │──►│   `alethe check` exits   │
                        │   only, local or s3://     │   │   nonzero on BOUNDED /   │
                        └───────────────────────────┘   │   UNACHIEVABLE           │
                                                         └─────────────────────────┘
```

Watermarks compose across lineage by **weakest link**: a downstream model's effective boundary is the most restrictive boundary among its upstreams; its effective evidence grade is the weakest on the path. `pit_report()` turns that into three zones:

| Zone | Condition | Meaning |
|---|---|---|
| **CERTAIN** | `since ≥ effective_boundary` | Fully retained — exact answer |
| **BOUNDED** | `earliest ≤ since < boundary` | Partially vacuumed — monotone aggregates are lower bounds |
| **UNACHIEVABLE** | `since < earliest` | An upstream didn't exist — the population itself is unknowable |

## Install

```bash
pip install -e ".[all]"        # everything
# or granular:
pip install -e ".[delta]"      # Delta Lake adapter
pip install -e ".[iceberg]"    # Apache Iceberg adapter
pip install -e ".[rewrite]"    # PIT SQL rewriting (sqlglot)
pip install -e ".[s3]"         # s3:// manifest persistence
```

Python 3.11+.

## Notebooks

Numbered in order of progression:

| Notebook | What it covers |
|---|---|
| [`00_idea_testing`](notebooks/00_idea_testing.ipynb) | The original exploration: all four implementation phases with intermediate outputs |
| [`01_library_quickstart`](notebooks/01_library_quickstart.ipynb) | The `alethe` API: semiring algebra, Delta + Iceberg watermarks, PIT achievability report |
| [`02_dbt_openlineage`](notebooks/02_dbt_openlineage.ipynb) | dbt manifest DAG → twice-temporal correction → OpenLineage emission + roundtrip |
| [`03_end_to_end`](notebooks/03_end_to_end.ipynb) | **The full story**: a dbt project on Delta + Iceberg → watermarks → PIT report → zone-gated query rewriting → empirical proof |
| [`04_dbt_pit_runs`](notebooks/04_dbt_pit_runs.ipynb) | A real dbt project (dbt-duckdb) with the macro shim: `dbt run --vars alethe_as_of`, snapshot vs source binding |
| [`05_airflow_reconstruction`](notebooks/05_airflow_reconstruction.ipynb) | Gated backfills: the naive failure, then skip / clamp+stamp / exact per zone — plus the production DAG |
| [`06_bounded_queries`](notebooks/06_bounded_queries.ipynb) | **How BOUNDED presents to an analyst**: verdict banner, OBSERVED rows + BEYOND candidates, `revenue ≥ $X` lower bounds vs temporal substitution |

All notebooks write their tables under the working directory — safe to delete, recreated on re-run.

## Using it

### dbt: run a model as of a point in time

Copy [`dbt_macros/alethe_pit.sql`](dbt_macros/alethe_pit.sql) into your project's `macros/` directory:

```bash
dbt run -s revenue_summary --vars '{"alethe_as_of": "2024-03-01"}'
```

With the var unset, compilation is byte-identical to stock dbt. Set, the shimmed `source()` appends engine-native time travel (`TIMESTAMP AS OF` on Spark/Databricks, `FOR TIMESTAMP AS OF` on Trino), and `ref()` of a **snapshot** wraps it in a `dbt_valid_from`/`dbt_valid_to` validity subquery — snapshots keep history in rows, so time-travelling the snapshot table itself would be a category error.

Declare each source's chain once in `schema.yml` so CI can resolve watermarks:

```yaml
sources:
  - name: raw
    tables:
      - name: orders
        meta:
          alethe:
            chain: delta://orders
```

### CI: refuse to ship a query that can't be answered

The macro binds the query but cannot know whether the target time is answerable. Gate it:

```bash
alethe check \
  --dbt-manifest target/manifest.json \
  --model revenue_summary \
  --as-of 2024-03-01 \
  --watermarks s3://audit-bucket/watermarks.jsonl \
  --run-results target/run_results.json
```

Exit codes: `0` CERTAIN · `1` BOUNDED (pass `--allow-bounded` to accept lower bounds) · `2` UNACHIEVABLE · `3` resolution error. The full PIT report is printed either way, including the twice-temporal materialization check. Add `--record` to land the verdict in the manifest as a `materialization-snapshot` entry.

For a standalone availability report — every model in the project (or one with `--model`), no `--as-of` needed:

```bash
alethe report --dbt-manifest target/manifest.json --watermarks watermarks.jsonl
```

### Airflow: refresh watermarks and publish lineage

```python
from datetime import datetime, timezone
import alethe
from alethe.integrations import to_run_event
import requests

def refresh_watermarks(**ctx):
    wm = alethe.watermark("s3://lake/orders")            # empirical oracle
    alethe.record(wm, "s3://audit-bucket/watermarks.jsonl")
    event = to_run_event(
        job_name="revenue_summary",
        job_namespace=ctx["dag"].dag_id,
        inputs=[wm],
        output_name="reporting.revenue_summary",
    )
    requests.post("http://marquez:5000/api/v1/lineage", json=event)
```

Downstream consumers reconstruct watermarks from the OpenLineage facets alone (`from_facet`) — no re-running the oracle.

## Files

| Path | What it is |
|---|---|
| `alethe/` | The installable package (adapters, semiring, manifest, lineage, CLI) |
| `alethe/integrations/` | dbt, OpenLineage, and the PIT SQL rewriter |
| `dbt_macros/alethe_pit.sql` | Drop-in dbt macro shim for `--vars alethe_as_of` |
| `notebooks/` | Executable notebooks, numbered by progression |
| `scripts/` | Standalone phase scripts (original reference runs) |
| `markdown/ows-spec-draft.md` | **The specification (v0.1.0-draft) — the normative document** |
| `markdown/alethe-value-proposition.md` | Positioning, compatibility matrix, steelmanned objections |

## License & Attribution

- **Code:** Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
- **Specification text** (`markdown/ows-spec-draft.md`): CC-BY 4.0 — attribution required per the spec header.
- **Trademarks & conformance claims:** see [TRADEMARK.md](TRADEMARK.md).
- **Citing this work:** DOI [10.5281/zenodo.21193962](https://doi.org/10.5281/zenodo.21193962) or GitHub's "Cite this repository" button (from `CITATION.cff`).

© 2026 Caelan Cooper.

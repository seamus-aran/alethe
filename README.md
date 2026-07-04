# Alethe

Reference implementation of the **Observability Watermark Specification (OWS v0.1.0-draft)**. From the Greek *aletheia* — truth as un-forgetting.

The unit of adoption is the spec, not this tool. Alethe exists to prove the spec is implementable across real, heterogeneous infrastructure.

## Abstract

Modern data platforms support point-in-time (PIT) queries over historical
state, but silently lose the ability to answer them as retention policies
destroy history. No current system distinguishes "this fact was absent at
time T" from "the evidence needed to answer this question no longer
exists." This specification defines the **observability watermark**: a
provable, per-chain statement of how far back in time a data asset can be
honestly queried, together with the evidence that justifies the claim. It
further defines how watermarks compose across lineage, how queries over
watermarked assets produce **verdicts** (exact, bounded, or refused), and
the integrity requirements that make those verdicts usable as audit
evidence.

The unit of adoption is this specification, not any implementation.

## The observability semiring

Alethe's query semantics are built on a three-valued semiring
`K = {ABSENT, BEYOND, OBSERVED}`:

```
OBSERVED  ─── fact is present and inside retention  (multiplicative identity)
   │
BEYOND    ─── query exceeds the retention boundary; honest refusal
   │
ABSENT    ─── fact is definitively not present      (additive identity)
```

Two operations compose annotations through relational algebra:

| ⊕ `(union)` | **ABSENT** | **BEYOND** | **OBSERVED** |   | ⊗ `(join)` | **ABSENT** | **BEYOND** | **OBSERVED** |
|---|---|---|---|---|---|---|---|---|
| **ABSENT**   | ABSENT  | BEYOND   | OBSERVED |   | **ABSENT**   | ABSENT | ABSENT | ABSENT   |
| **BEYOND**   | BEYOND  | BEYOND   | OBSERVED |   | **BEYOND**   | ABSENT | BEYOND | BEYOND   |
| **OBSERVED** | OBSERVED| OBSERVED | OBSERVED |   | **OBSERVED** | ABSENT | BEYOND | OBSERVED |

**Intuition:** `⊕` is max — if any derivation path is OBSERVED, the row is observed. `⊗` is min — a join is only as knowable as its least knowable conjunct. `BEYOND` taint enters at a vacuumed source and propagates through every downstream join and projection by algebra alone, with no special-case logic in the query engine.

## Setup

Python 3.11+ required.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Running

### Notebook (recommended)

```bash
pip install -e ".[all]" jupyter
jupyter notebook notebooks/
```

The notebooks are numbered in order of progression:

| Notebook | What it covers |
|---|---|
| `00_idea_testing.ipynb` | The original exploration: all four implementation phases with intermediate outputs |
| `01_library_quickstart.ipynb` | The `alethe` API: semiring algebra, Delta + Iceberg watermarks, PIT achievability report |
| `02_dbt_openlineage.ipynb` | dbt manifest DAG → twice-temporal correction → OpenLineage emission + roundtrip |
| `03_end_to_end.ipynb` | **The full story**: a dbt project on Delta + Iceberg → watermarks → PIT report → zone-gated time-travel query rewriting → empirical proof |

All notebooks write their tables under the working directory — safe to delete, recreated on re-run.

### Scripts directly

Run in order (Phase 2 must precede Phase 3, which reads the Delta table Phase 2 writes):

```bash
python scripts/obs_semiring.py
python scripts/ows_examples.py
python scripts/ows_delta_oracle.py
python scripts/ows_manifest_and_iceberg.py
```

## Running dbt as of a point in time

Copy [`dbt_macros/alethe_pit.sql`](dbt_macros/alethe_pit.sql) into your dbt project's `macros/` directory, then:

```bash
dbt run -s revenue_summary --vars '{"alethe_as_of": "2024-03-01"}'
```

With the var unset, compilation is byte-identical to stock dbt. With it set, the shimmed `source()` appends engine-native time travel (`TIMESTAMP AS OF` on Spark/Databricks, `FOR TIMESTAMP AS OF` on Trino), and `ref()` of a **snapshot** wraps it in a `dbt_valid_from`/`dbt_valid_to` validity subquery — snapshots keep history in rows, so time-travelling the snapshot table itself would be a category error.

Gate it in CI with the PIT report first: the macro binds the query but cannot know whether the target time is CERTAIN, BOUNDED, or UNACHIEVABLE — that's `DbtLineage.pit_report()`'s job. The library-side equivalent (with zone gating built in) is `DbtLineage.rewrite_model()`.

## Files

| Path | What it is |
|---|---|
| `alethe/` | Installable Python package — `pip install -e ".[all]"` |
| `notebooks/` | Jupyter notebooks (quickstart, integrations, full exploration) |
| `scripts/` | Standalone phase scripts (original reference runs) |
| `markdown/ows-spec-draft.md` | The specification (v0.1.0-draft) — the normative document |
| `markdown/alethe-value-proposition.md` | Positioning, compatibility matrix, steelmanned objections |
| `ows_manifest.jsonl` | Persisted manifest written by the full exploration notebook |

## License & Attribution

- **Code:** Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
- **Specification text** (`ows-spec-draft.md`): CC-BY 4.0 — attribution required per the spec header.
- **Trademarks & conformance claims:** see [TRADEMARK.md](TRADEMARK.md) *(pending)*.
- **Citing this work:** [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.21193962.svg)](https://doi.org/10.5281/zenodo.21193962) or use GitHub's "Cite this repository" button (from `CITATION.cff`).

© 2026 Caelan Cooper.

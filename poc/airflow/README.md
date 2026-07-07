# POC — Honest reconstruction across history, in real Airflow

Proof-of-concept: an Airflow backfill gated by alethe's PIT report rebuilds
deep history honestly, while the ungated baseline fails at runtime. All
evidence in [`poc_airflow_history.ipynb`](poc_airflow_history.ipynb) is from
**real execution**: real Delta tables, real `VACUUM`, real Airflow 3.3 task
runs via `dag.test()`.

## The setup

Two watermarked Delta chains with different vacuum histories, plus a static
date dimension:

```
dim_customer:  v0──v1 ................................. v2 ──VACUUM──   ← limits BOUNDED
fct_orders:          d0──d1──...──d9 ──VACUUM── d10──...──d19           ← limits UNACHIEVABLE
                     └── destroyed ──┘└──── survives ────┘
dim_date:      static — deliberately unwatermarked (no retention risk)
```

The gated DAG ([`dags_src/reconstruct_revenue.py`](dags_src/reconstruct_revenue.py))
has three tasks — `resolve_zone → rebuild_partition → validate_partition` —
so the gating is visible in real task states. `naive_reconstruct` is the
ungated baseline.

## Findings

| # | Claim | Evidence |
|---|---|---|
| A1 | Naive backfills fail at runtime with no warning | real Airflow run ended `failed`; `FileNotFoundError` on a vacuumed parquet shown raw first |
| A2 | Gating turns that into plan-time decisions | 8 real runs: 2 skipped, 3 bounded, 3 exact — zero task failures |
| A3 | Task states form an audit grid | [`img/honesty_map.png`](img/honesty_map.png) from real TaskInstance states |
| A4 | Partial evidence stays labelled forever | `pit_verdict` column stamped in every output partition |
| A5 | Every decision is tamper-evident | manifest chain INTACT: 2 watermarks + 8 materialization-snapshots, one per backfill date |
| A6 | Composition is weakest-link, per zone | UNACHIEVABLE limited by `fct_orders` (birth), BOUNDED limited by `dim_customer` (late vacuum) |

The timeline ([`img/timeline.png`](img/timeline.png)) shows the lie measured:
the transaction log lists 20 write versions; only 11 are physically readable.

## Reproduce

Airflow's pins conflict with dbt's, so it gets its own venv (from the repo root):

```bash
python3 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow>=2.9" deltalake pyarrow pandas matplotlib ipykernel nbconvert
.venv-airflow/bin/pip install -e .
cd poc/airflow
../../.venv-airflow/bin/python -m nbconvert --to notebook --execute --inplace poc_airflow_history.ipynb
```

Airflow 3 notes baked into the notebook: the metadata DB needs `airflow db
migrate` once, and the DAG file must live inside `$AIRFLOW_HOME/dags` for
`dag.test()` to serialize it.

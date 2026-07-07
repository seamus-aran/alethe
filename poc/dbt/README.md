# Alethe × dbt proof of concept

Point-in-time honesty across a real dbt DAG whose sources live in **two open table
formats**: a real Delta Lake table (delta-rs) that really gets `VACUUM`ed, and a real
Apache Iceberg table (pyiceberg + `SqlCatalog`) whose old data files really get
destroyed. Everything is driven from one executable notebook,
[`poc_dbt_history.ipynb`](poc_dbt_history.ipynb), and every scenario outcome is
**assertion-enforced** — a clean execution *is* the proof.

## Layering

```
delta://orders  (REAL Delta table)     ─┐
iceberg://raw.returns (REAL Iceberg)   ─┼─ source layer (watermarked by alethe,
raw.customers  (mutable dimension)     ─┘  annotated via meta.alethe.chain)
        │                │                      │
   stg_orders       stg_returns          customers_snapshot (SCD2, 4 real runs)
        │                │                      │ stg_customers
        └── fct_orders ──┘                dim_customer
                 └────── revenue_by_segment ─────┘   (+ dim_date seed)
```

- **Staging is ephemeral** on purpose: the physical leaf relations appear verbatim in
  the fact/dim layer's compiled SQL, which is exactly what point-in-time binding
  (rewriter or macro shim) needs to reach them.
- **dbt executes on duckdb**, so the duckdb `raw` schema mirrors the *current* state of
  the lake tables (duckdb has no native Delta/Iceberg time travel). In production the
  sources **are** the lake tables. The watermarks always come from the real
  Delta/Iceberg tables, never from the mirror.

## Scenarios and outcomes

| # | Scenario | Outcome (asserted in the notebook) |
|---|---|---|
| S1 | **The lie** | `_delta_log` lists 22 versions (20 daily writes + vacuum commits); every write before v9 fails a *real read* — metadata alone lies about the destroyed prefix |
| S2 | **The oracle** | `alethe.watermark()` derives boundary v9 from two independent derivations (file existence vs `VACUUM END` commits), then validates it empirically: read v9 OK, read v8 fails. Bonus pitfall, shown deliberately: `load_as_version(timestamp)` resolves into vacuumed versions and only fails at read time |
| S3 | **Reports** | `alethe report` over the real `target/manifest.json` resolves every model via `meta.alethe.chain`; twice-temporal check from `run_results.json` is CONFORMANT; `alethe check` CI gate exits 1 on BOUNDED and 0 with `--allow-bounded --record` (signed override into the hash chain) |
| S4 | **Rewrites** | `rewrite_model(dialect='spark')`: CERTAIN → both sources bound with `TIMESTAMP AS OF`; BOUNDED → rewrite succeeds with an attached warning; UNACHIEVABLE → `UnachievableQueryError`; snapshot upstream → SCD2 validity predicate instead of time travel; and the mart that reads only materialized tables refuses to pretend (nothing bound, unmatched tracked reported) |
| S5 | **Macro shim** | `dbt compile` without vars is byte-identical to stock dbt; with `--vars '{"alethe_as_of": …, "alethe_as_of_style": "spark"}'` sources gain ` TIMESTAMP AS OF …` and the snapshot ref becomes a validity-window subquery (visible in `dim_customer`, which inlines the ephemeral staging) |
| S6 | **Snapshots vs storage** | Day-by-day probe of one mutated customer (C03: trial → consumer → smb → enterprise): storage time travel on the vacuumed Delta mirror fails with a real read error for 7 of 20 days, while the SCD2 snapshot reconstructs all 20 |
| S7 | **Weakest link** | Iceberg suffix boundary with readable islands (empty delete-snapshots between destroyed appends) recorded in `proof` but never claimed; `iceberg://raw.returns` limits `fct_orders`; the youngest chain, `snapshot://customers_snapshot` (grade `witnessed-fresh`), limits `revenue_by_segment` and drags its effective grade down |

Visuals (regenerated on every execution, also displayed inline):

- `img/v1_orders_timeline.png` — the 20 day-versions on real commit timestamps, green
  dots readable / red × destroyed, vacuum event, empirical boundary, and the
  UNACHIEVABLE / BOUNDED / CERTAIN zone bands.
- `img/v2_snapshot_vs_storage.png` — C03's segment as a step plot from SCD2 validity
  windows, with the storage boundary and the destroyed-evidence region shaded: snapshot
  coverage extends left of what storage can still prove.

## Reproduce

```bash
cd poc/dbt
/Users/seamusaran/Documents/alethe/.venv/bin/python -m nbconvert \
    --to notebook --execute --inplace poc_dbt_history.ipynb
```

(or any `python -m jupyter nbconvert …` from a venv with `alethe`, `deltalake`,
`pyiceberg`, `dbt-duckdb`, `duckdb`, `pandas`, `pyarrow`, `sqlglot`, `matplotlib`
installed). The run takes roughly 2–4 minutes: the builds deliberately `sleep` between
commits so every Delta version and Iceberg snapshot has a distinct real timestamp.

The notebook is idempotent — it wipes and rebuilds `data/` (Delta lakehouse, Iceberg
warehouse, duckdb file, watermark ledger) and `project/target/` on every execution.
Notebook working directory must be `poc/dbt/` (nbconvert's default when invoked as
above).

### Expected warnings (correct behavior, not noise)

- alethe's `DbtLineage` warns that `customers_snapshot` uses `strategy='check'`:
  check-strategy snapshots witness state at run time and cannot reconstruct between-run
  states. That is precisely why the snapshot chain is graded `witnessed-fresh` with
  boundary = first run rather than `derived`.
- The S2 timestamp pitfall cell *intentionally* provokes a read failure to show that
  `load_as_version(timestamp)` resolves into vacuumed versions.

## Files

| Path | What it is |
|---|---|
| `poc_dbt_history.ipynb` | The executable narrative: claim → executed evidence → conclusion per scenario |
| `poc_support.py` | Machinery the notebook calls: builds/vacuums the lake tables, drives dbt via subprocess, probes storage vs SCD2, renders the charts |
| `project/` | The dbt project (duckdb profile): source → staging (ephemeral) → fact/dim → mart, `check`-strategy snapshot, `dim_date` seed, and the alethe macro shim (`macros/alethe_pit.sql`) |
| `project/models/schema.yml` | Sources annotated with `meta.alethe.chain` (`delta://orders`, `iceberg://raw.returns`) — how watermark chains bind to dbt nodes |
| `data/` *(generated)* | Delta tables (`lakehouse/orders`, `lakehouse/customers`), Iceberg warehouse, duckdb database, and the hash-chained `watermarks.jsonl` |
| `img/` | The two generated charts |

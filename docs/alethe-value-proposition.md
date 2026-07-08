# Alethe
### Retention-aware temporal queries that refuse honestly
*Implementing the Observability Watermark Specification (OWS). From the Greek* aletheia *— truth as un-forgetting: the disclosure of what has and hasn't been forgotten.*

## One-liner

Querying the past should be as cheap and honest as querying the present — and when history has been vacuumed away, the engine should say *"unknowable,"* not *"no rows."*

## The problem

Every modern lakehouse quietly lies about the past.

Delta Lake, Iceberg, and Fabric all support time travel — until VACUUM runs. Once files older than the retention window are removed, queries against that history either fail with an opaque error or, far worse, **return partial results that look complete**. A point-in-time join across two tables with *different* retention windows is the nightmare case: one side of the join silently loses history, and the query returns a confident, well-formed, wrong answer. No engine today distinguishes "this row did not exist at time T" from "we destroyed the evidence that would let us answer that."

This is not an edge case. It is the default behavior of every AS OF query that touches a watermark:

- **Auditors and regulators** get reconstructions of past state with no indication of which parts are reconstruction and which are fabrication-by-omission.
- **ML teams** train on point-in-time feature joins where label leakage's evil twin — *silent feature loss* — corrupts training sets invisibly.
- **Finance and compliance** re-run historical reports whose totals change not because the past changed, but because retention policy did.
- **Debugging engineers** conclude "the bug wasn't there at T" when the truth is "we can no longer see T."

The industry's answer is documentation: *"be careful, VACUUM breaks time travel."* Care is not a semantics.

## The thesis

**Unknowability should be a value, not an error.** Specifically: a third truth value — beyond OBSERVED and ABSENT — meaning *"this question exceeds the observability boundary,"* propagated through every relational operator by algebra rather than by special-case code.

The mechanism is a three-element semiring:

- **⊕ (union / projection):** any observed derivation wins; but "beyond retention" beats "absent" — if the only alternative to no is *we can't know*, the honest answer is *we can't know*.
- **⊗ (join):** absence annihilates; unknowability taints. A joined row is only as knowable as its least knowable input.

Because these obey the semiring laws (machine-verifiable in milliseconds), the entire relational algebra — selects, projects, joins, unions, and every query plan built from them — automatically computes, for each output row, whether it is an **answer**, a **non-answer**, or a **refusal**. Zero refusal logic in the operators. The taint from a single vacuumed table flows through a five-way join correctly because distributivity says it must.

## Why this doesn't exist yet

The two intellectual lineages that should have produced this never met:

1. **Annotated databases** (provenance semirings, 2007; Uncertainty-Annotated Databases, SIGMOD 2019) established semiring propagation of epistemic status through queries — but model uncertainty of *value*, not retention-induced unknowability, and have no temporal dimension.
2. **Temporal vacuuming theory** (Skyt, Jensen & Mark, 2002) established that queries over vacuumed history need correctness semantics and user-facing signals of absence — but predates the lakehouse and offers procedural criteria, not compositional algebra.

Meanwhile the lakehouse ecosystem built time travel and VACUUM as engineering features with no query-level semantics of their loss. The gap is the intersection: **per-table retention watermarks as first-class algebraic values in multi-table temporal joins.** Both parent literatures exist; the synthesis does not.

## What the user gets

- **Honest AS OF queries.** Every result partitions into confirmed answers and explicit refusals, with refusals carrying the row-shapes that *might* have qualified. "Neither confirmed nor denied" becomes machine-readable.
- **Refusal as a compiler guarantee, not a runtime surprise.** Because watermarks are metadata, a temporal-join compiler can prove at plan time whether a query is fully answerable, partially answerable, or unanswerable — before scanning a byte.
- **Retention policy becomes a governance surface.** The cost of shortening a retention window is no longer invisible; it is the precise set of queries that start refusing. Retention tuning gets a feedback loop.
- **Trustworthy training data.** Point-in-time correct feature joins that certify their own completeness — or flag exactly which entities fell outside observability.
- **No engine rewrite.** The annotation layer composes over existing Delta/Iceberg time travel; watermarks are already tracked (log retention, deleted-file retention). This is semantics on top of infrastructure that exists.

## What this is not

- Not SQL NULL. NULL means "a value exists but is unknown." BEYOND means "the question itself exceeds what was retained." Conflating them is how forty years of three-valued-logic pain happened.
- Not probabilistic databases. No distributions, no possible-worlds enumeration cost. Three values, two operation tables, exhaustively verifiable.
- Not an argument for infinite retention. The opposite: it makes *finite* retention safe to rely on, because its boundaries become visible instead of silent.

## Target compatibility

The system standardizes at the metadata layer — the only layer these platforms share. Each target is a thin adapter implementing one contract: `watermark(table) → (boundary, evidence_grade, proof)`. Formats differ only in how the watermark is justified, graded on the evidence lattice:

- **Derived** — an append-only log proves the boundary continuously
- **Derived-countersigned** — the system self-reports its retention state via catalog introspection; a named human confirms at setup; heartbeat re-introspection detects drift and flags contradicted watermarks
- **Witnessed** — no native log; a checksummed observation attests state at heartbeat intervals; inter-witness gaps are themselves observability boundaries

| Target | Watermark source | Evidence grade |
|---|---|---|
| **Delta Lake** (OSS + Fabric) | `_delta_log`: VACUUM START/END commits, log retention, checkpoint truncation | Derived *(reference adapter)* |
| **Apache Iceberg** | Snapshot lineage in `metadata.json`; `expire_snapshots` / `remove_orphan_files` history; immutable snapshot IDs as audit anchors | Derived |
| **Databricks** | Delta log per table + Unity Catalog table properties for retention policy | Derived |
| **Snowflake** (native tables) | Time Travel + Fail-safe boundaries via `DATA_RETENTION_TIME_IN_DAYS` and account-parameter introspection | Derived-countersigned |
| **Snowflake** (external stages) | Stage file checksums / ETags as integrity witnesses | Witnessed |
| **BigQuery** | Time-travel window and table snapshot lineage via `INFORMATION_SCHEMA` introspection | Derived-countersigned |
| **PostgreSQL** | `pg_current_wal_lsn()`, replication slot `restart_lsn`, `wal_keep_size` catalog introspection; derived when fronted by CDC | Derived-countersigned |
| **Oracle** | `V$ARCHIVED_LOG`, `V$LOG_HISTORY`, RMAN retention policy introspection (via LogMiner-sanctioned surfaces, never raw redo) | Derived-countersigned |
| **MySQL** | `SHOW BINARY LOGS`, `binlog_expire_logs_seconds` introspection | Derived-countersigned |
| **CDC streams** (Debezium, Qlik Replicate) | Earliest retained position (LSN/SCN/offset) as opaque monotonic token; snapshot and gap events in the stream | Derived |
| **Kafka topics** | Earliest retained offset per partition (retention = VACUUM); compaction tracked as state-vs-history destruction | Derived |
| **Raw Parquet / object storage / DuckDB** | File-listing + checksum heartbeat; DuckDB doubles as the zero-cluster verification engine | Witnessed |

Design invariants across all targets: positions are ordered tokens, never parsed proprietary formats; boundaries are comparable only within a single chain (no implied cross-source total order); witnesses are only ever created in the present (retroactive attestations enter as a distinct, weaker `imported-attestation` grade); and watermarks move only forward — no operation may rewind a boundary or edit an evidence link.

Adoption is retroactive to each log's own horizon: derived targets get their full surviving history on day one; witnessed targets accrue history from first heartbeat. A conformance test suite grades any implementation — including ones we didn't write — making the spec, not the tool, the unit of adoption.

## The bigger claim

Data systems have spent two decades making answers cheaper. Almost nothing has been spent making answers *honest about their own limits*. As retention policies tighten (cost, GDPR, right-to-be-forgotten) while historical queries multiply (ML, audit, agents querying data autonomously), the gap between "what the engine returns" and "what is actually knowable" is widening — and every consumer downstream inherits the confusion.

A query engine that can say *"I refuse, and here is precisely why"* is more trustworthy than one that always answers. That property is worth more every year, and it costs three truth values and two small tables of algebra.

## Appendix A: Objections, steelmanned

### "History should be managed in the system of record."

This is real architectural doctrine, not laziness: OLTP systems own transactional truth; analytical copies making historical claims risks dual sources of truth; if you need audit history, enable CDC or archiving at the source. Taken seriously, it collapses for analytics and ML on four independent grounds, each sufficient:

**1. The systems of record can't do it.** Postgres retains WAL for days, not years. SaaS sources expose current-state APIs with no time travel at all. Long-horizon flashback on commercial databases is prohibitively expensive at analytical scale. The doctrine assumes a capability that mostly doesn't exist and that no one will fund.

**2. The question isn't answerable there even in principle.** Analytical questions are cross-system — "customer state at the moment the order shipped" joins CRM, order management, and billing. No single system of record contains that join, so no single system of record can own its history. Point-in-time truth about *composed* state can only live where composition happens: the analytical layer. The history in question does not exist in any SOR.

**3. ML already voted.** Point-in-time-correct feature joins and training-serving skew are why feature stores exist. Nobody replays five OLTP systems' logs to build a training set. The industry decided, with its infrastructure spend, that historical state for ML lives in the analytical layer. This project doesn't propose that move — it makes the move that already happened honest.

**4. Even retained history can't be queried at the source.** Audit reconstructions against production OLTP are operationally forbidden in every serious shop. Historical replicas are mandatory for workload isolation alone — so the trust problem over replicas exists regardless of doctrine.

**The reversal:** the critic is right that the analytical copy is epistemically weaker than the source — and today that weakness is unmanaged and invisible. Warehouses answer AS OF queries with identical confidence whether the evidence is a complete transaction log or a re-snapshot after a CDC gap. This system is the only proposal that takes the critic's concern seriously: it quantifies how much weaker the copy is (evidence grades), refuses when the copy cannot honestly answer, and alerts when source retention silently stops supporting downstream claims. The achievable purist position was never "keep history in the SOR" — it is "never claim more than your evidence supports," which is this specification.

**The honest concession:** for single-system, row-level audit questions ("what did this account record say on March 3rd"), a source-anchored CDC archive genuinely is the better answer — and the evidence lattice says so, ranking the source-anchored chain above the analytical replica. That case was never this project's market; conceding it is the lattice working as designed.

### "Isn't this just NULL / probabilistic databases / data observability tooling?"

Addressed in "What this is not" above, but the category confusion recurs enough to restate the boundary with adjacent tooling: lineage tells you *where* data came from; quality monitors tell you whether *current* data looks anomalous. Neither tells you *how far back the trail is trustworthy* — the observability boundary is empty space between those categories, not a competitor within them.

### "Refusals will just get disabled."

Correct, if refusal is binary. The first Friday a refusal blocks a dashboard, someone will turn the gate off. The design answer is graduated governance: per-query-class evidence thresholds, and an override-with-attestation path that converts "disable the seatbelt" into "sign for the exception" — preserving the evidence trail precisely at the moment it is being overridden.

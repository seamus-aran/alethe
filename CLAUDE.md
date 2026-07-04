# Alethe

Reference implementation of the **Observability Watermark Specification
(OWS v0.1.0-draft)**. From the Greek *aletheia* — truth as un-forgetting.

The unit of adoption is the **spec**, not this tool. Alethe exists to prove the
spec is implementable across real, heterogeneous infrastructure.

## Core concept

The **observability watermark**: a provable, per-chain statement of how far back
a data asset can be *honestly* queried. It distinguishes "this fact was absent at
time T" from "the evidence needed to answer that question was destroyed." Every
modern lakehouse silently conflates these once VACUUM runs; OWS makes the
boundary legible and refuses honestly when a query crosses it.

Thesis: **unknowability should be a value, not an error.** This makes *finite*
retention safe to rely on — it is not an argument for infinite retention.

## Core model (spec §2)

Watermark contract, one function per adapter:

```
watermark(table) -> { boundary, evidence_grade, proof, claim_recorded_at }
```

- **Boundary uses SUFFIX semantics**: the earliest position from which *all later
  positions* are readable. Readability is NOT monotone in position — destroyed
  and intact states can interleave (Iceberg delete/append pairs leave empty
  intermediate snapshots readable between destroyed ones). Readable "islands"
  below the boundary are recorded in `proof` but MUST NOT extend the claim.
- **Boundaries are validated empirically**, never by metadata arithmetic alone:
  a read at the boundary must succeed and a read at boundary−1 must fail.
- **`claim_recorded_at` ≠ boundary.** "Attested since" and "true since" are
  different statements; conflating them is a conformance failure.

Evidence grade lattice (strongest first):
`derived` > `derived-countersigned` > `witnessed-fresh` > `witnessed-stale`
> `imported-attestation`. Witnesses are only ever minted in the present;
retroactive evidence enters only as `imported-attestation`.

## Query semantics (spec §3)

Three-valued **observability semiring** `K = {ABSENT=0, BEYOND, OBSERVED=1}`:
- `⊕` (union / projection / alternative derivation) = **max** under
  `ABSENT < BEYOND < OBSERVED`
- `⊗` (join / conjunction) = **min** under the same order
- `0 = ABSENT` annihilates `⊗`; `1 = OBSERVED` is its identity

`BEYOND` = "the question exceeds a retention boundary." **Distinct from SQL
NULL** (NULL = a value exists but is unknown). Refusal propagates by algebra
alone — zero special-case logic in operators. Conforming impls machine-verify
the semiring laws (exhaustive = 126 identities).

`BEYOND` rows are **candidates**: known row-shapes annotated `BEYOND`, values
carried as-recorded. The *annotation*, not value masking, conveys epistemic
status — masking join keys is non-conforming because it makes refusals silently
vanish from joins.

**Verdicts**: `EXACT` / `BOUNDED` / `REFUSED`. Every non-EXACT verdict MUST name
its limiting link(s). Monotone queries (select, project, join, union, SUM/COUNT
over non-negative measures) degrade to `BOUNDED`. Non-monotone queries
(negation, anti-join, NOT EXISTS, MIN/MAX over possibly-incomplete populations)
degrade to `REFUSED` — and SHOULD offer the monotone complement. Overrides only
via named attestation ("sign for the exception," never "disable the gate").

## Manifest & lineage (spec §4, §6)

- Append-only, **hash-chained** JSONL ledger; each entry commits to its
  predecessor's hash. Corrections are new entries, never edits.
- **Custody separation** is required: if the same principal can mutate both data
  and manifest, it is a dashboard, not evidence — and the impl must say so.
- Watermarks are **monotone** (never rewind), which is what permits
  coordination-free merging across replicas.
- **Weakest-link composition**: downstream effective watermark = most restrictive
  boundary on the path; effective grade = weakest grade on the path.
- **Twice-temporal correction**: a downstream table AS OF t reflects upstream
  state as of the *last materialization before t*, not t. Ignoring materialization
  lag certifies stale results as exact and is non-conforming.
- **Write-time evidence evaluation**: grade is evaluated at write time and
  snapshotted into the manifest (`materialization-snapshot`); a row keeps its
  grade even after upstream is later vacuumed.
- Gap dispositions: `open` / `attested-gap` / `restored` / `escrowed`.

Conformance levels (cumulative): **OWS-Core** → **OWS-Verdict** → **OWS-Lineage**.

## Files

| File | What it proves |
|---|---|
| `ows_delta_oracle.py` | Real Delta table (delta-rs), real VACUUM. Two-phase oracle: metadata replay of `_delta_log` + empirical time-travel validation. Demonstrates the log listing versions that are physically unreadable. |
| `ows_manifest_and_iceberg.py` | Delta boundary cross-checked via two independent derivations (file existence vs VACUUM commits). Plus a real Iceberg adapter (pyiceberg + sqlite catalog) proving the *same contract* across a second metadata model. One contract, two formats, one ledger. |
| `obs_semiring.py` | K-relations with exhaustive semiring-law verification. `TemporalTable.as_of()` with retention watermarks. Demo of `BEYOND` taint flowing through joins and surviving projection. |
| `ows_examples.py` | Four simulation-grade reference scenarios: oracle, manifest tamper detection, contradicted-watermark heartbeat, verdict engine (incl. escrow + twice-temporal). |
| `ows-spec-draft.md` | The specification itself (v0.1.0-draft). The normative document. |
| `alethe-value-proposition.md` | Positioning, target compatibility matrix, steelmanned objections. |
| `ows_manifest.jsonl` | Persisted manifest: two validated watermark entries (Delta v7; Iceberg suffix boundary with 3 readable islands). |

**Status**: Phase 1 complete. The oracle + manifest + Iceberg files run against
*real* infrastructure (not simulation); `ows_examples.py` is simulation-grade
(real logic, fake infra) covering mechanisms not yet wired to live systems.

## Runtime

```
pip install deltalake pyiceberg pyarrow pandas
```

- `ows_delta_oracle.py` and `ows_manifest_and_iceberg.py` write real tables under
  `/home/claude/...` in their current form — **adjust these paths** for a local
  run (they hardcode the sandbox filesystem).
- `obs_semiring.py` and `ows_examples.py` are pure-Python, no infra deps.
- Run order for the real pipeline: `ows_delta_oracle.py` first (builds the Delta
  table the manifest step reads), then `ows_manifest_and_iceberg.py`.

## Positioning

Novelty = the intersection of **provenance semirings** (Green, Karvounarakis,
Tannen 2007) and **Uncertainty-Annotated DBs** (Feng et al., SIGMOD 2019) with
**temporal vacuuming theory** (Skyt, Jensen & Mark 2002). The annotated-DB
lineage models uncertainty of *value* with no temporal dimension; the temporal
lineage predates the lakehouse and offers procedural criteria, not compositional
algebra. Neither lineage tooling (where data came from) nor quality monitoring
(is current data anomalous) answers *how far back the trail is trustworthy.*

**Honest concession**: for single-system, row-level audit questions, a
source-anchored CDC archive genuinely is the better answer — and the evidence
lattice ranks it above the analytical replica. That's the lattice working as
designed, not a gap.

## Open problems (spec §9)

- **Unknown population**: distinguishing "known entity, unknowable state" from
  "unknowable population" (checkpoint truncation). May need a table-level refusal
  state or a 4th carrier element.
- General aggregate bounds beyond non-negative monotone measures (AU-DB style).
- Cross-chain temporal reasoning (any relaxation of the no-total-order rule).
- Formal composition proofs that verdict computation over lineage preserves
  semiring soundness.

## Working conventions

- Keep positions as **ordered opaque tokens**; never parse proprietary log
  formats — use each system's sanctioned interfaces.
- Positions are comparable **only within a single chain**. No implied cross-chain
  total order.
- When adding an adapter: derive boundary + empirically validate + record proof.
  The empirical validation is the conformance requirement, not optional.
# Observability Watermark Specification (OWS)

**Version:** 0.1.0-draft
**Author:** Caelan Cooper
**First published:** 2026-07-04
**DOI:** 10.5281/zenodo.21193962
**Status:** DRAFT — not yet stable; breaking changes permitted until 1.0.0
**Reference implementation:** Alethe

© 2026 Caelan Cooper. This specification is licensed under the
[Creative Commons Attribution 4.0 International License (CC-BY 4.0)](https://creativecommons.org/licenses/by/4.0/).

Required attribution for reuse or adaptation:
> "Observability Watermark Specification (OWS) by Caelan Cooper,
> [DOI], licensed CC-BY 4.0. [Note changes if any.]"

---

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

## 1. Conventions

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be
interpreted as described in RFC 2119.

- **Table** — any queryable data asset (managed table, file set, stream).
- **Chain** — an ordered, append-only sequence of evidence about a table's
  state over time (a transaction log, snapshot lineage, CDC stream, or
  witness sequence).

- **Position** — an opaque, monotonically increasing token within a single
chain (version number, LSN, SCN, Kafka offset, witness sequence number).

- **Boundary** — the earliest position within a chain from which *all
subsequent positions* permit honest PIT claims (suffix semantics).
Readability is not necessarily monotone in position: destroyed and
intact states can interleave (e.g., Iceberg delete/append snapshot pairs
leave empty intermediate snapshots readable between destroyed ones).
Readable positions below the boundary ("islands") MUST be recorded in
the watermark proof and MAY be served, but MUST NOT extend the boundary
claim.

- **Watermark** — the tuple `(boundary, evidence_grade, proof)` for a chain.

- **Manifest** — the append-only, tamper-evident ledger in which watermark
claims and related events are recorded.

- **Verdict** — the completeness classification of a query result: EXACT,
  BOUNDED, or REFUSED.

## 2. Core model

### 2.1 The observability boundary

Every table has, at every moment, a true boundary: the earliest point in
time for which its retained evidence permits reconstruction of state. The
boundary exists whether or not it is measured. OWS makes it legible.

An implementation MUST treat positions as ordered opaque tokens. It MUST
NOT parse proprietary log formats directly; it MUST use each system's
sanctioned interfaces (catalog views, metadata files, decoding APIs).

Positions are comparable only within a single chain. A manifest MUST NOT
assert or imply a total order across chains. Cross-chain reasoning occurs
only at the lineage layer (§6) via explicit edges.

### 2.2 The watermark contract

Every adapter implements one function:

```
watermark(table) -> {
  boundary:        { position | timestamp },
  evidence_grade:  Grade,
  proof:           opaque evidence artifact(s),
  claim_recorded_at: timestamp   // when THIS system recorded the claim
}
```

`claim_recorded_at` MUST be distinguished from the boundary itself: a
boundary may be derivable retroactively (from a surviving log), but the
integrity of the claim is inherited from the source's guarantees until
the moment the manifest recorded it. "Attested since" and "true since"
are different statements; conflating them is a conformance failure.

### 2.3 Evidence grades

Grades form a partially ordered lattice of evidence strength. The
normative grades, strongest first:

| Grade | Meaning |
|---|---|
| `derived` | An append-only log controlled by the platform continuously proves the boundary (Delta `_delta_log`, Iceberg snapshot lineage, CDC positions, Kafka offsets). |
| `derived-countersigned` | The source self-reports its retention state via introspection; a named identity confirmed the interpretation; heartbeat re-introspection monitors for drift (OLTP catalogs, Snowflake/BigQuery retention parameters). |
| `witnessed-fresh` | No native log; a checksummed observation attests state, and the most recent witness is within the declared heartbeat SLA. |
| `witnessed-stale` | The heartbeat SLA has elapsed since the last witness; the boundary is untrustworthy until re-witnessed. |
| `imported-attestation` | Evidence created outside this system (a saved inventory report, a backup manifest) admitted retroactively. Weakest grade: chain of custody is unverifiable. |

Requirements:

- Witnesses MUST only be created in the present. An implementation MUST
  NOT mint a witness for a past state. Retroactive evidence MUST enter
  only as `imported-attestation`.
- Time between consecutive witnesses is itself an observability gap: an
  implementation MUST NOT make state claims about inter-witness intervals
  beyond what the bounding witnesses jointly entail.
- A `derived-countersigned` watermark whose current introspection
  disagrees with its confirmed boundary enters the **contradicted**
  state: verdicts MUST use the more conservative boundary, and the
  implementation MUST surface the contradiction for re-confirmation.
- Countersignatures MUST bind a real identity (e.g., SSO principal) and
  timestamp, and MUST store the raw introspection output they confirm.
  Implementations MUST NOT represent countersignatures as cryptographic
  non-repudiation unless a signature scheme actually provides it.

### 2.4 Gap dispositions

A gap (an interval beyond a boundary, or between witnesses) MAY carry a
disposition recorded in the manifest:

| Disposition | Meaning |
|---|---|
| `open` | Default. Unaddressed gap. |
| `attested-gap` | A named identity recorded a justification (e.g., GDPR erasure, documented retention policy). The gap remains visible but is marked compliant. |
| `restored` | Rows re-derived from an upstream system of record were written to fill the gap. Restored rows are annotated as such — reconstructed evidence, distinct from and weaker than retained evidence. |
| `escrowed` | Coarse-grained artifacts (aggregates, counts, hashes) were persisted before destruction, permitting bounded answers within the gap at reduced granularity. |

Dispositions never delete the gap record. Remediation operates on gap
metadata; destroyed evidence is not recoverable and implementations MUST
NOT claim otherwise.

## 3. Query semantics

### 3.1 The observability semiring

Row-level status is drawn from the three-element commutative semiring
K = {ABSENT, BEYOND, OBSERVED} with:

- ⊕ (union/projection/alternative derivation) = max under the order
  ABSENT < BEYOND < OBSERVED
- ⊗ (join/conjunction) = min under the same order
- 0 = ABSENT (annihilates ⊗), 1 = OBSERVED (identity of ⊗)

Interpretation: ABSENT = definitively not present; OBSERVED = present
inside retention; BEYOND = the question exceeds an observability
boundary. BEYOND is distinct from SQL NULL: NULL means "a value exists
but is unknown"; BEYOND means "the question itself exceeds what was
retained."

Relational operators over K-annotated relations MUST be defined purely by
these operations; refusal behavior MUST NOT be implemented as
special-case logic inside operators. Conforming implementations SHOULD
machine-verify the semiring laws over the carrier (exhaustive
verification is 126 identities).

Rows in BEYOND territory are represented as **candidates**: known
row-shapes annotated BEYOND, with values carried as-recorded. The
annotation, not value masking, conveys epistemic status; masking join
keys is non-conforming because it causes refusals to silently vanish
from joins.

### 3.2 Verdicts

Every query over watermarked assets MUST return a verdict:

- **EXACT** — every chain link in the query's lineage is inside its
  boundary at the requested time, at acceptable grade, with
  materialization lag accounted for (§6.2).
- **BOUNDED** — some link is degraded, and the query is monotone (§3.3):
  the result carries a certain lower part (from OBSERVED evidence) and an
  upper part (OBSERVED plus BEYOND candidates). For row sets: certain
  rows vs. candidate rows. For aggregates over non-negative measures:
  numeric `[at_least, at_most]` intervals (at_most MAY be unbounded when
  population knowledge is destroyed).
- **REFUSED** — no honest bound exists (non-monotone query past a
  boundary, contradicted watermark at required grade, or requested time
  precedes all evidence).

Every non-EXACT verdict MUST name its **limiting link(s)**: the specific
chain, boundary, and grade that determined the verdict. A refusal that
does not explain itself is non-conforming.

Query policies MAY set per-query-class evidence thresholds (e.g.,
compliance queries require `derived` or `witnessed-fresh`; exploration
accepts anything). Overrides MUST be possible only via attestation: a
named identity accepting a degraded verdict, recorded in the manifest.
An override converts "disable the gate" into "sign for the exception."

### 3.3 Monotonicity classification

Implementations MUST classify queries by monotonicity to select the
strongest honest verdict:

- **Monotone** operators (selection, projection, join, union, SUM/COUNT
  over non-negative measures): additional evidence can only add results.
  Degraded evidence yields BOUNDED.
- **Non-monotone** operators (negation, anti-join, NOT EXISTS,
  set-difference, MIN/MAX over possibly incomplete populations,
  universal quantification): absence of evidence beyond a boundary is
  not evidence of absence, so no safe lower bound exists past a
  boundary. Degraded evidence yields REFUSED for the non-monotone
  claim. Implementations SHOULD offer the monotone complement (e.g.,
  refuse "customers who did not order"; certify "customers who did,
  back to the boundary").
- DISTINCT-count and top-K inherit the unknown-population problem:
  at-least is safe; at-most requires population knowledge and is
  otherwise unbounded.

Classification is static (per-operator over the query plan) and MUST be
decidable without executing the query.

## 4. The manifest

The manifest is the ledger of ledgers. Requirements:

- **Append-only and hash-chained.** Each entry MUST include the hash of
  its predecessor. Entries MUST NOT be edited or deleted; corrections
  are new entries referencing the corrected one.
- **Custody separation.** The manifest MUST be storable outside the
  administrative domain of the platforms it describes. If the same
  principal can mutate both data and manifest, the manifest is a
  dashboard, not evidence, and the implementation MUST say so in its
  conformance statement.
- **Monotone.** Watermarks move only forward. No operation may rewind a
  boundary or reorder a chain. (This monotonicity is what permits
  coordination-free merging of manifests across replicas and catalogs.)
- **Serializable.** Entries MUST be representable in JSON with no
  required runtime dependencies, so that any party can implement a
  reader.

Illustrative entry:

```json
{
  "seq": 4182,
  "prev_hash": "b3a1…",
  "recorded_at": "2026-07-04T17:20:11Z",
  "kind": "watermark",
  "chain_id": "delta://lakehouse/sales/orders",
  "boundary": { "version": 912, "timestamp": "2026-06-04T00:00:00Z" },
  "evidence_grade": "derived",
  "proof": { "type": "delta-log-excerpt", "artifacts": ["…"] },
  "claim_recorded_at": "2026-07-04T17:20:11Z"
}
```

Other entry kinds: `witness`, `countersignature`, `contradiction`,
`gap-disposition`, `materialization-snapshot` (§6.2), `override`,
`lineage-edge`.

## 5. Adapters

An adapter is any component fulfilling the watermark contract for a class
of systems. Normative sourcing rules:

- Log-bearing formats (Delta, Iceberg): derive from log/snapshot
  metadata, reconciling log retention, file retention, and checkpoint
  truncation. The correct boundary is the earliest version that is
  *readable*, not merely *listed* — a log entry whose data files were
  vacuumed does not extend the boundary. Adapters MUST validate
  boundaries empirically (queries at boundary succeed; at boundary−1
  fail) as part of conformance.
- CDC streams (Debezium-class, Qlik-class): boundary = earliest retained
  position; snapshot events establish witnessed baselines; connector
  gaps exceeding source log retention MUST be recorded as BEYOND
  intervals with a new baseline following.
- Log transports (Kafka): retention deletion is boundary movement;
  compaction MUST be recorded as history-destruction that preserves
  state observability (latest-per-key) while destroying path
  observability. These are distinct claims and MUST NOT be conflated.
- Introspectable systems (OLTP catalogs, warehouse retention
  parameters): suggestion via read-only introspection, confirmation via
  countersignature, drift detection via heartbeat (§2.3). Suggestions
  MUST show their reasoning (which artifact limits the boundary) so
  confirmation is of a reasoned claim, not a bare number.
- Unmanaged storage (raw Parquet, object stores, file drops): witness
  heartbeat only. Witnesses attest and detect drift; they MUST NOT
  interpret changes (no delta inference, no footer diffing). A witness
  that interprets is a bad transaction log and non-conforming.

## 6. Lineage composition

### 6.1 Weakest link

Lineage edges are declared (pipeline configs, dbt manifests,
OpenLineage) — an OWS implementation consumes lineage; it MUST NOT
manufacture it. For a downstream asset, the effective watermark along a
path is the most restrictive boundary on the path, and the effective
grade is the weakest grade on the path. The verdict for a downstream
query is computed against effective watermarks of all contributing
paths.

### 6.2 Twice-temporal correction

A downstream table AS OF time t reflects upstream state as of the last
materialization before t, not as of t. Implementations MUST incorporate
materialization history: EXACT requires both (a) t within every
retention boundary on every path and (b) materialization lag at every
hop accounted for. An implementation that ignores materialization lag
will certify stale results as exact and is non-conforming.

### 6.3 Write-time evidence evaluation

Evidence is evaluated as of write time, not query time. At every
materialization, the implementation MUST snapshot the effective upstream
watermarks into the manifest (`materialization-snapshot`). A row written
while its upstream chain was green retains its grade even after upstream
evidence is later vacuumed, provided the downstream table's own chain
proves when the row was written (chain of custody).

Consequences implementations MUST model:

- Incremental materializations are temporally non-uniform: observation
  timestamps are per-row/partition/batch (write-time stamps such as a
  shared `run_ts` are the anchoring mechanism).
- Incremental tables can extend observability beyond upstream retention
  (an accidental archive); full-replace tables re-inherit only current
  upstream evidence at each run and retain history only through their
  own chain. Materialization strategy is therefore an
  observability-horizon decision and SHOULD be surfaced as such.

## 7. Conformance

Three levels, cumulative:

- **OWS-Core**: manifest integrity (§4), watermark contract (§2.2),
  grades and their invariants (§2.3), at least one adapter with
  empirical boundary validation.
- **OWS-Verdict**: semiring semantics (§3.1), verdict computation with
  limiting-link explanation (§3.2), monotonicity classification (§3.3),
  override-with-attestation.
- **OWS-Lineage**: composition (§6.1), twice-temporal correction (§6.2),
  write-time evaluation and materialization snapshots (§6.3), gap
  dispositions (§2.4).

A public conformance test suite grades implementations and adapters.
Implementations MUST publish a conformance statement declaring level,
adapters, and custody model.

## 8. Security considerations

The witness holds broad read access; its compromise enables false
attestation. Deployments MUST scope witness credentials read-only and
SHOULD isolate witness identity from platform administrators. Manifest
custody separation (§4) is the primary defense against retroactive
falsification; external anchoring of the hash chain (e.g., periodic
publication of chain heads) SHOULD be supported. Countersignature
identity claims are only as strong as the binding identity provider.

## 9. Open problems (non-normative)

- **Unknown population**: distinguishing "known entity, unknowable
  state" from "unknowable population" (e.g., checkpoint truncation
  destroying knowledge of which entities existed). Likely requires a
  table-level refusal state or a fourth carrier element.
- **Aggregate bounds beyond non-negative monotone measures**:
  AU-DB-style attribute-interval semantics for general aggregation.
- **Cross-chain temporal reasoning**: any relaxation of the
  no-total-order rule (§2.1) — e.g., via hybrid logical clocks — is out
  of scope for 1.0.
- **Composition proofs**: formal verification that verdict computation
  over lineage preserves the semiring soundness guarantees.

## 10. Versioning

Semantic versioning of the specification. Until 1.0.0, breaking changes
are permitted with migration notes. From 1.0.0, manifest entries MUST
carry a spec version, and readers MUST reject entries from unknown major
versions rather than guess.

## Appendix: prior art (non-normative)

Provenance semirings (Green, Karvounarakis, Tannen 2007) establish
semiring-annotated relations. Uncertainty-Annotated Databases (Feng et
al., SIGMOD 2019) establish propagation of certainty bounds through
queries; AU-DBs extend to attribute-level intervals. Skyt, Jensen & Mark
(2003) establish correctness semantics for queries over vacuumed
temporal databases. OWS occupies their intersection: retention-induced
unknowability as a first-class algebraic value, per-chain and composed
across lineage, in multi-engine environments. The distinction from
adjacent tooling: lineage records where data came from; quality
monitoring evaluates current data; OWS states how far back the trail is
trustworthy.

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
pip install jupyter
jupyter notebook alethe.ipynb
```

The notebook walks through four stages in order:

| Stage | What it proves |
|---|---|
| 0 — Algebraic foundation | Three-valued semiring `K = {ABSENT, BEYOND, OBSERVED}`. 126 semiring laws verified exhaustively. `BEYOND` taint propagates through joins by algebra alone. |
| 1 — Simulation grade | Oracle, tamper-evident manifest, contradicted watermark, and verdict engine against simulated infrastructure. |
| 2 — Real Delta Lake | Real Delta table, real `VACUUM`, real time-travel reads. Proves the `_delta_log` advertises versions that are physically unreadable after vacuum. |
| 3 — Manifest + Iceberg | Delta boundary cross-checked via two independent derivations, then the same contract against a real Iceberg table. One contract, two formats, one ledger. |

Phases 2 and 3 write to `./lakehouse/` and `./iceberg_warehouse/` — both are safe to delete and will be recreated on re-run.

### Scripts directly

Run in order (Phase 2 must precede Phase 3, which reads the Delta table Phase 2 writes):

```bash
python scripts/obs_semiring.py
python scripts/ows_examples.py
python scripts/ows_delta_oracle.py
python scripts/ows_manifest_and_iceberg.py
```

## Files

| File | What it proves |
|---|---|
| `alethe.ipynb` | Full exploratory notebook — all four stages with intermediate outputs |
| `scripts/obs_semiring.py` | K-relations with exhaustive semiring-law verification |
| `scripts/ows_examples.py` | Four simulation-grade reference scenarios |
| `scripts/ows_delta_oracle.py` | Real Delta table, real VACUUM, two-phase oracle |
| `scripts/ows_manifest_and_iceberg.py` | Manifest integration + real Iceberg adapter |
| `ows-spec-draft.md` | The specification (v0.1.0-draft) — the normative document |
| `alethe-value-proposition.md` | Positioning, compatibility matrix, steelmanned objections |
| `ows_manifest.jsonl` | Persisted manifest written by Phase 3 |

## License & Attribution

- **Code:** Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
- **Specification text** (`ows-spec-draft.md`): CC-BY 4.0 — attribution required per the spec header.
- **Trademarks & conformance claims:** see [TRADEMARK.md](TRADEMARK.md) *(pending)*.
- **Citing this work:** [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.21193962.svg)](https://doi.org/10.5281/zenodo.21193962) or use GitHub's "Cite this repository" button (from `CITATION.cff`).

© 2026 Caelan Cooper.

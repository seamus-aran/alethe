# Contributing to Alethe

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all]" pytest
pytest
```

The tests build real Delta tables and destroy history with a real VACUUM
— empirical validation is the conformance requirement, so please keep new
tests empirical too (no metadata-arithmetic-only assertions).

## Developer Certificate of Origin

Contributions require a DCO sign-off certifying you have the right to
submit the work under the project license. Add `-s` to your commits:

```bash
git commit -s -m "your message"
```

By signing off you agree to the [Developer Certificate of Origin](https://developercertificate.org/).

Contributions are accepted under:
- **Code:** Apache License 2.0
- **Specification text:** CC-BY 4.0

## What to contribute

The spec is the primary artifact. Code contributions that prove a new
adapter (a new storage engine satisfying the watermark contract) or
strengthen the semiring implementation are most valuable.

Before opening a PR, check the open problems in `ows-spec-draft.md §9`
— those are the known unsolved questions where contributions matter most.

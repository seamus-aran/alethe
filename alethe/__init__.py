# Copyright 2026 Caelan Cooper
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Alethe — Observability Watermark Specification reference library.

Quick start:

    import alethe

    # Delta Lake (auto-detected)
    wm = alethe.watermark("path/to/delta/table")

    # Apache Iceberg (explicit adapter + catalog)
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog("local", uri="sqlite:///catalog.db",
                         warehouse="file:///path/to/warehouse")
    wm = alethe.watermark("db.table_name", adapter="iceberg", catalog=catalog)

    # Persist to a hash-chained manifest
    alethe.record(wm, "watermarks.jsonl")

    # Check a query's epistemic status
    from datetime import datetime, timezone
    v = alethe.verdict(wm, since=datetime(2024, 1, 1, tzinfo=timezone.utc))
    print(v)  # Verdict(EXACT) or Verdict(BOUNDED, limiting=['delta://...'])
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path

from ._models import (
    EvidenceGrade, UnachievableQueryError, Verdict, VerdictStatus, Watermark,
    PitReport, PitStatus, PitZone,
)
from ._manifest import Manifest
from ._semiring import K, KRelation, QueryResult, TemporalTable, split_result, verify_semiring_laws
from ._lineage import pit_report
from ._asof import AsOfResult, asof
from . import integrations

__all__ = [
    # top-level functions
    "watermark",
    "record",
    "record_report",
    "load_watermarks",
    "verdict",
    "pit_report",
    "asof",
    "AsOfResult",
    # integrations
    "integrations",
    # models
    "Watermark",
    "EvidenceGrade",
    "UnachievableQueryError",
    "Verdict",
    "VerdictStatus",
    "PitReport",
    "PitStatus",
    "PitZone",
    # manifest
    "Manifest",
    # semiring
    "K",
    "KRelation",
    "TemporalTable",
    "QueryResult",
    "split_result",
    "verify_semiring_laws",
]

__version__ = "0.1.0"


def watermark(table: str | Path, *, adapter: str | None = None,
              **kwargs) -> Watermark:
    """Derive and empirically validate an OWS watermark.

    Parameters
    ----------
    table:
        Filesystem path for Delta tables, or ``"namespace.table_name"``
        for Iceberg tables.
    adapter:
        ``"delta"`` or ``"iceberg"``. Auto-detected from the presence of
        ``_delta_log/`` when omitted.
    **kwargs:
        Passed to the adapter. Iceberg requires ``catalog=<SqlCatalog>``.
    """
    path = Path(table)
    if adapter == "delta" or (adapter is None and (path / "_delta_log").exists()):
        from .adapters.delta import DeltaAdapter
        return DeltaAdapter().watermark(path)
    if adapter == "iceberg":
        catalog = kwargs.get("catalog")
        if catalog is None:
            raise ValueError(
                "Iceberg adapter requires catalog=<pyiceberg catalog>. "
                "Example: SqlCatalog('local', uri='sqlite:///catalog.db', "
                "warehouse='file:///path/to/warehouse')")
        from .adapters.iceberg import IcebergAdapter
        return IcebergAdapter().watermark(str(table), catalog=catalog)
    raise ValueError(
        f"Cannot auto-detect adapter for {table!r}. "
        "Pass adapter='delta' or adapter='iceberg'.")


def _as_manifest(manifest: str | Path | Manifest) -> Manifest:
    """Accept a path or an already-open Manifest (reused across a loop
    to avoid re-reading the file per append)."""
    return manifest if isinstance(manifest, Manifest) else Manifest(manifest)


def record(wm: Watermark, manifest: str | Path | Manifest) -> dict:
    """Append a watermark entry to a hash-chained JSONL manifest.

    The manifest is created if it does not exist. Returns the new entry.
    """
    return _as_manifest(manifest).append("watermark", **wm.to_dict())


def load_watermarks(manifest: str | Path | Manifest) -> dict[str, Watermark]:
    """Load the latest watermark per chain from a recorded manifest.

    Verifies the hash chain first; a tampered manifest raises rather than
    yielding claims that can no longer be trusted.

    Returns
    -------
    dict mapping ``chain`` → its most recent ``Watermark``.  Watermarks
    are monotone (spec §4), so the latest entry per chain is the current
    claim.
    """
    m = _as_manifest(manifest)
    if not m.verify():
        raise ValueError(
            f"Manifest {manifest} failed hash-chain verification — "
            "entries have been edited or reordered. Refusing to load.")
    out: dict[str, Watermark] = {}
    for e in m.entries:
        if e.get("kind") != "watermark":
            continue
        if "boundary_dt" not in e:
            raise ValueError(
                f"Manifest entry seq={e.get('seq')} predates boundary_dt "
                "persistence — re-record it with alethe >= 0.1.0.")
        out[e["chain"]] = Watermark.from_dict(e)
    return out


def record_report(report: "PitReport", manifest: str | Path | Manifest,
                  as_of: datetime | None = None) -> dict:
    """Persist a PIT report as a ``materialization-snapshot`` manifest entry.

    This is the spec §4 write-time evidence snapshot: the report's zones,
    effective boundary, and grade are committed to the hash chain at the
    moment they were computed.  Even if upstream tables are vacuumed
    further tomorrow, this entry proves what was knowable — and with what
    evidence — when the model was built or the check was run.

    Parameters
    ----------
    report:
        The PIT report to persist.
    manifest:
        Path (local or ``s3://``) of the hash-chained manifest, or an
        already-open :class:`Manifest`.
    as_of:
        Optional query time this report was evaluated against (e.g. the
        CI ``--as-of`` or the backfill logical date).  When given, the
        zone verdict at that time is stored alongside the report.
    """
    m = _as_manifest(manifest)
    payload: dict = {
        "model": report.name,
        "effective_boundary": report.effective_boundary.isoformat(),
        "earliest_dt": report.earliest_dt.isoformat(),
        "limiting_chain": report.limiting_chain,
        "effective_grade": report.effective_grade,
        "upstream_chains": [wm.chain for wm in report.upstreams],
        "zones": [
            {"status": z.status.value,
             "start": z.start.isoformat() if z.start else None,
             "end": z.end.isoformat() if z.end else None,
             "limiting_chains": z.limiting_chains}
            for z in report.zones
        ],
    }
    if report.materialization_dt is not None:
        payload["materialization_dt"] = report.materialization_dt.isoformat()
        payload["materialization_conformant"] = report.materialization_conformant
    if as_of is not None:
        payload["as_of"] = as_of.isoformat()
        payload["verdict_at_as_of"] = report.query(as_of).status.value
    return m.append("materialization-snapshot", **payload)


def verdict(wm: Watermark, since: datetime) -> Verdict:
    """Return the epistemic verdict for a query reaching back to ``since``.

    ``EXACT``   — query is fully within the observability boundary.
    ``BOUNDED`` — query crosses the boundary; monotone aggregates are
                  valid lower bounds, but the answer is not complete.
                  Non-monotone queries (NOT EXISTS, MIN/MAX over possibly
                  incomplete sets) should be treated as ``REFUSED``.

    Note: this is a two-state check against a single watermark and does
    not distinguish UNACHIEVABLE (``since`` before the table existed).
    For three-zone gating use ``pit_report(name, [wm]).query(since)``.

    Parameters
    ----------
    wm:
        The watermark for the table being queried.
    since:
        The earliest point-in-time the query needs data from. Must be
        timezone-aware.
    """
    if since >= wm.boundary_dt:
        return Verdict(VerdictStatus.EXACT, [], wm)
    return Verdict(VerdictStatus.BOUNDED, [wm.chain], wm)

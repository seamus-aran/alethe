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
    EvidenceGrade, Verdict, VerdictStatus, Watermark,
    PitReport, PitStatus, PitZone,
)
from ._manifest import Manifest
from ._semiring import K, KRelation, QueryResult, TemporalTable, split_result, verify_semiring_laws
from ._lineage import pit_report

__all__ = [
    # top-level functions
    "watermark",
    "record",
    "verdict",
    "pit_report",
    # models
    "Watermark",
    "EvidenceGrade",
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


def record(wm: Watermark, manifest: str | Path) -> dict:
    """Append a watermark entry to a hash-chained JSONL manifest.

    The manifest is created if it does not exist. Returns the new entry.
    """
    m = Manifest(Path(manifest))
    return m.append(
        "watermark",
        chain=wm.chain,
        boundary=wm.boundary,
        evidence_grade=wm.evidence_grade,
        empirically_validated=wm.empirically_validated,
        proof=wm.proof,
        claim_recorded_at=wm.claim_recorded_at.isoformat(),
        readable_islands=wm.readable_islands,
    )


def verdict(wm: Watermark, since: datetime) -> Verdict:
    """Return the epistemic verdict for a query reaching back to ``since``.

    ``EXACT``   — query is fully within the observability boundary.
    ``BOUNDED`` — query crosses the boundary; monotone aggregates are
                  valid lower bounds, but the answer is not complete.
                  Non-monotone queries (NOT EXISTS, MIN/MAX over possibly
                  incomplete sets) should be treated as ``REFUSED``.

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

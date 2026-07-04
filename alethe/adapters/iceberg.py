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

"""Apache Iceberg adapter for the OWS watermark contract.

Requires: pip install alethe[iceberg]
"""

from __future__ import annotations
from datetime import datetime, timezone


class IcebergAdapter:
    """Derive and empirically validate an OWS watermark for an Iceberg table.

    Boundary uses SUFFIX semantics: the earliest snapshot from which all
    later snapshots are physically readable. Because Iceberg delete/append
    pairs leave trivially-readable empty intermediate snapshots, readability
    is not monotone — readable islands below the boundary are recorded in
    `proof` but do NOT extend the claim.
    """

    def watermark(self, table_name: str, catalog) -> "Watermark":  # type: ignore[name-defined]
        from .._models import Watermark, EvidenceGrade

        tbl = catalog.load_table(table_name)
        snapshots = sorted(tbl.metadata.snapshots, key=lambda s: s.sequence_number)
        probes = [(snap, *self._snapshot_readable(tbl, snap)) for snap in snapshots]

        candidate = None
        for i, (snap, _ok, _) in enumerate(probes):
            if all(ok2 for _, ok2, _ in probes[i:]):
                candidate = snap
                break

        if candidate is None:
            raise RuntimeError(
                f"No readable suffix found in Iceberg table '{table_name}'.")

        validated = all(
            ok for s, ok, _ in probes
            if s.sequence_number >= candidate.sequence_number)

        islands = [
            {"snapshot_id": str(s.snapshot_id),
             "sequence_number": s.sequence_number}
            for s, ok, _ in probes
            if ok and s.sequence_number < candidate.sequence_number
        ]

        boundary_dt = datetime.fromtimestamp(
            candidate.timestamp_ms / 1000, tz=timezone.utc)
        earliest_dt = datetime.fromtimestamp(
            snapshots[0].timestamp_ms / 1000, tz=timezone.utc)

        return Watermark(
            chain=f"iceberg://{table_name}",
            boundary={
                "snapshot_id": str(candidate.snapshot_id),
                "sequence_number": candidate.sequence_number,
                "timestamp_ms": candidate.timestamp_ms,
            },
            boundary_dt=boundary_dt,
            earliest_dt=earliest_dt,
            evidence_grade=EvidenceGrade.DERIVED,
            empirically_validated=validated,
            readable_islands=islands,
            proof={
                "snapshots_listed": len(snapshots),
                "snapshots_readable": sum(1 for _, ok, _ in probes if ok),
                "per_snapshot": {
                    str(s.snapshot_id): (
                        f"{'READABLE' if ok else 'UNREADABLE'} ({note})"
                    )
                    for s, ok, note in probes
                },
            },
        )

    def _snapshot_readable(self, tbl, snap) -> tuple[bool, str]:
        try:
            n = tbl.scan(snapshot_id=snap.snapshot_id).to_arrow().num_rows
            return True, f"read {n} rows"
        except Exception as e:
            return False, type(e).__name__

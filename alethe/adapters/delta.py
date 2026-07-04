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

"""Delta Lake adapter for the OWS watermark contract.

Requires: pip install alethe[delta]
"""

from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path


class DeltaAdapter:
    """Derive and empirically validate an OWS watermark for a Delta table.

    Two-phase oracle:
      Phase A — replay `_delta_log` commit JSONs to find the earliest
                version whose full file set physically exists on disk.
      Phase B — attempt real time-travel reads at the candidate and at
                candidate−1 to confirm the boundary empirically.

    A vacuum-commit cross-check uses the log's own VACUUM END records as
    a second independent derivation, elevating confidence when both agree.
    """

    def watermark(self, table: str | Path) -> "Watermark":  # type: ignore[name-defined]
        from .._models import Watermark, EvidenceGrade

        table = Path(table)
        per_version = self._replay_log(table)
        boundary = self._derive_boundary(per_version, table)
        validated = self._empirically_validate(table, boundary, sorted(per_version))
        corroborated, vac_versions, boundary_by_log = \
            self._vacuum_crosscheck(table, per_version, boundary)
        boundary_dt = self._commit_timestamp(table, boundary)

        return Watermark(
            chain=f"delta://{table.name}",
            boundary={"version": boundary},
            boundary_dt=boundary_dt,
            evidence_grade=EvidenceGrade.DERIVED,
            empirically_validated=validated,
            proof={
                "derivation_file_existence": boundary,
                "derivation_vacuum_commits": boundary_by_log,
                "corroborated": corroborated,
                "vacuum_end_versions": vac_versions,
            },
        )

    # ------------------------------------------------------------------

    def _replay_log(self, table: Path) -> dict[int, set[str]]:
        log_dir = table / "_delta_log"
        live: set[str] = set()
        per_version: dict[int, set[str]] = {}
        for commit in sorted(log_dir.glob("*.json")):
            version = int(commit.stem)
            for line in commit.read_text().splitlines():
                try:
                    action = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "add" in action:
                    live.add(action["add"]["path"])
                elif "remove" in action:
                    live.discard(action["remove"]["path"])
            per_version[version] = set(live)
        return per_version

    def _derive_boundary(self, per_version: dict[int, set[str]],
                         table: Path) -> int:
        for v in sorted(per_version):
            if all((table / f).exists() for f in per_version[v]):
                return v
        raise RuntimeError(
            f"No readable version found in {table} — table may be fully vacuumed.")

    def _empirically_validate(self, table: Path, boundary: int,
                               versions: list[int]) -> bool:
        return (self._readable(table, boundary) and
                (boundary - 1 not in versions or
                 not self._readable(table, boundary - 1)))

    def _readable(self, table: Path, version: int) -> bool:
        try:
            from deltalake import DeltaTable
            DeltaTable(str(table), version=version).to_pyarrow_table()
            return True
        except Exception:
            return False

    def _vacuum_crosscheck(self, table: Path, per_version: dict,
                           boundary: int) -> tuple[bool, list[int], int | None]:
        try:
            from deltalake import DeltaTable
            history = DeltaTable(str(table)).history()
            vac_ends = [h for h in history if h.get("operation") == "VACUUM END"]
            vac_versions = [h["version"] for h in vac_ends]
            if vac_ends:
                last_vac = max(vac_versions)
                writes_before = [h["version"] for h in history
                                 if h.get("operation") == "WRITE"
                                 and h["version"] < last_vac]
                boundary_by_log: int | None = max(writes_before) if writes_before else None
            else:
                boundary_by_log = min(per_version)
            return boundary_by_log == boundary, vac_versions, boundary_by_log
        except Exception:
            return False, [], None

    def _commit_timestamp(self, table: Path, version: int) -> datetime:
        log_file = table / "_delta_log" / f"{version:020d}.json"
        if log_file.exists():
            for line in log_file.read_text().splitlines():
                try:
                    action = json.loads(line)
                    if "commitInfo" in action:
                        ts_ms = action["commitInfo"].get("timestamp", 0)
                        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                except Exception:
                    continue
        return datetime.now(tz=timezone.utc)

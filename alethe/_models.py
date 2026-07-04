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

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class EvidenceGrade(str, Enum):
    DERIVED = "derived"
    DERIVED_COUNTERSIGNED = "derived-countersigned"
    WITNESSED_FRESH = "witnessed-fresh"
    WITNESSED_STALE = "witnessed-stale"
    IMPORTED_ATTESTATION = "imported-attestation"


class VerdictStatus(str, Enum):
    EXACT = "EXACT"
    BOUNDED = "BOUNDED"
    REFUSED = "REFUSED"


@dataclass
class Watermark:
    chain: str
    boundary: dict
    boundary_dt: datetime
    earliest_dt: datetime           # oldest known position (table creation / first snapshot)
    evidence_grade: EvidenceGrade
    empirically_validated: bool
    proof: dict
    claim_recorded_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc))
    readable_islands: list[dict] = field(default_factory=list)


@dataclass
class Verdict:
    status: VerdictStatus
    limiting_links: list[str]
    watermark: Watermark

    def __repr__(self) -> str:
        links = f", limiting={self.limiting_links}" if self.limiting_links else ""
        return f"Verdict({self.status.value}{links})"


class PitStatus(str, Enum):
    CERTAIN = "CERTAIN"           # since >= effective_boundary: fully retained
    BOUNDED = "BOUNDED"           # earliest_dt <= since < effective_boundary: retained with gaps
    UNACHIEVABLE = "UNACHIEVABLE" # since < earliest_dt: table didn't exist yet


@dataclass
class PitZone:
    status: PitStatus
    start: datetime | None        # None = beginning of time
    end: datetime | None          # None = open (now / future)
    limiting_chains: list[str]


@dataclass
class PitReport:
    """Point-in-time achievability report for a (downstream) model."""
    name: str
    upstreams: list["Watermark"]
    effective_boundary: datetime      # weakest-link: max(boundary_dt)
    earliest_dt: datetime             # max(earliest_dt) — latest start across upstreams
    limiting_chain: str               # chain that imposed the effective_boundary
    effective_grade: "EvidenceGrade"  # weakest grade on the path
    zones: list[PitZone]
    materialization_dt: datetime | None = None  # from dbt run_results.json

    @property
    def materialization_conformant(self) -> bool | None:
        """True iff the model was materialized while upstream data was within
        retention.  None when materialization time is unknown.

        False means the model was built from data that was already partially
        vacuumed — a spec §6 twice-temporal conformance failure.
        """
        if self.materialization_dt is None:
            return None
        return self.materialization_dt >= self.effective_boundary

    def query(self, since: datetime) -> PitZone:
        """Return the achievability zone for a query reaching back to `since`."""
        for zone in self.zones:
            start_ok = zone.start is None or since >= zone.start
            end_ok = zone.end is None or since < zone.end
            if start_ok and end_ok:
                return zone
        return self.zones[-1]  # UNACHIEVABLE is always last

    def __str__(self) -> str:
        sep = "─" * 56
        lines = [
            f"PIT Achievability Report: {self.name}",
            sep,
            f"{'Upstream chain':<36} {'Boundary':<24} Grade",
        ]
        for wm in self.upstreams:
            marker = " ← limiting" if wm.chain == self.limiting_chain else ""
            lines.append(
                f"  {wm.chain:<34} {str(wm.boundary_dt)[:19]:<24} "
                f"{wm.evidence_grade}{marker}")
        lines += [
            sep,
            f"Effective boundary:  {str(self.effective_boundary)[:19]}  "
            f"(limiting: {self.limiting_chain})",
            f"Effective grade:     {self.effective_grade}",
        ]
        if self.materialization_dt is not None:
            conformant = self.materialization_conformant
            status = "CONFORMANT" if conformant else "NON-CONFORMANT ⚠"
            lines.append(
                f"Materialization:     {str(self.materialization_dt)[:19]}  "
                f"[twice-temporal: {status}]")
            if not conformant:
                lines.append(
                    "  !! Model was materialized before the upstream retention "
                    "boundary. Results may embed vacuumed data. (spec §6)")
        lines += ["", "PIT zones:"]
        for z in self.zones:
            start = str(z.start)[:19] if z.start else "−∞"
            end = str(z.end)[:19] if z.end else "now"
            note = f"  limiting: {z.limiting_chains}" if z.limiting_chains else ""
            lines.append(f"  {z.status.value:<14} {start} → {end}{note}")
        return "\n".join(lines)

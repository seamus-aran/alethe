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

"""Weakest-link composition and PIT achievability reporting (spec §4, §6)."""

from __future__ import annotations
from ._models import (
    EvidenceGrade,
    PitReport,
    PitStatus,
    PitZone,
    Watermark,
)

# Grade ordering: lower index = stronger evidence.
_GRADE_ORDER = [
    EvidenceGrade.DERIVED,
    EvidenceGrade.DERIVED_COUNTERSIGNED,
    EvidenceGrade.WITNESSED_FRESH,
    EvidenceGrade.WITNESSED_STALE,
    EvidenceGrade.IMPORTED_ATTESTATION,
]


def _weaker(a: EvidenceGrade, b: EvidenceGrade) -> EvidenceGrade:
    return a if _GRADE_ORDER.index(a) >= _GRADE_ORDER.index(b) else b


def pit_report(name: str, upstreams: list[Watermark]) -> PitReport:
    """Build a PIT achievability report using weakest-link composition.

    Three zones (spec §4 weakest-link + §9 unknown-population):

    CERTAIN      since >= effective_boundary
                 All upstream history is retained; the answer is exact.

    BOUNDED      earliest_dt <= since < effective_boundary
                 Data exists for this period but retention has destroyed
                 some of it. Monotone aggregates (SUM, COUNT) return
                 valid lower bounds. Non-monotone queries are REFUSED.

    UNACHIEVABLE since < earliest_dt
                 At least one upstream table did not yet exist. The
                 population itself is unknowable — not just vacuumed.

    Parameters
    ----------
    name:
        Logical name of the downstream model being described.
    upstreams:
        Watermarks of all direct upstream tables. For a single table,
        pass a one-element list.
    """
    if not upstreams:
        raise ValueError("upstreams must contain at least one Watermark")

    # Weakest-link boundary: the most restrictive (latest) boundary_dt.
    limiting_wm = max(upstreams, key=lambda w: w.boundary_dt)
    effective_boundary = limiting_wm.boundary_dt

    # Effective grade: the weakest (lowest-confidence) grade on the path.
    effective_grade = upstreams[0].evidence_grade
    for wm in upstreams[1:]:
        effective_grade = _weaker(effective_grade, wm.evidence_grade)

    # Earliest data point: max(earliest_dt) — for a join, we need rows
    # from ALL upstreams, so we can't go earlier than the latest start.
    earliest = max(upstreams, key=lambda w: w.earliest_dt)
    earliest_dt = earliest.earliest_dt

    # Limiting chains for BOUNDED zone.
    limiting_chains = [wm.chain for wm in upstreams
                       if wm.boundary_dt >= effective_boundary]

    zones: list[PitZone] = [
        PitZone(
            status=PitStatus.CERTAIN,
            start=effective_boundary,
            end=None,
            limiting_chains=[],
        ),
        PitZone(
            status=PitStatus.BOUNDED,
            start=earliest_dt,
            end=effective_boundary,
            limiting_chains=limiting_chains,
        ),
        PitZone(
            status=PitStatus.UNACHIEVABLE,
            start=None,
            end=earliest_dt,
            limiting_chains=[earliest.chain],
        ),
    ]

    return PitReport(
        name=name,
        upstreams=upstreams,
        effective_boundary=effective_boundary,
        earliest_dt=earliest_dt,
        limiting_chain=limiting_wm.chain,
        effective_grade=effective_grade,
        zones=zones,
    )

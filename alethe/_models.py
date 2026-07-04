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

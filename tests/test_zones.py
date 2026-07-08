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

"""Spec §4/§6: weakest-link composition, PIT zones, twice-temporal."""

from datetime import datetime, timedelta, timezone

import pytest

from alethe import PitStatus, VerdictStatus, pit_report, verdict
from alethe._models import EvidenceGrade, Watermark, parse_dt


def _wm(chain, earliest, boundary, grade=EvidenceGrade.DERIVED):
    return Watermark(
        chain=chain,
        boundary={"version": 1},
        boundary_dt=boundary,
        earliest_dt=earliest,
        evidence_grade=grade,
        empirically_validated=True,
        proof={},
    )


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def test_weakest_link_composition():
    a = _wm("delta://orders", T0, T0 + timedelta(days=10))
    b = _wm("iceberg://returns", T0 + timedelta(days=3),
            T0 + timedelta(days=20), grade=EvidenceGrade.WITNESSED_STALE)
    rep = pit_report("mart", [a, b])
    # boundary: most restrictive (latest); earliest: latest start (join
    # needs rows from ALL upstreams); grade: weakest on the path.
    assert rep.effective_boundary == b.boundary_dt
    assert rep.limiting_chain == b.chain
    assert rep.earliest_dt == b.earliest_dt
    assert rep.effective_grade == EvidenceGrade.WITNESSED_STALE


def test_three_zones_and_gating():
    rep = pit_report("m", [_wm("delta://t", T0, T0 + timedelta(days=10))])
    assert [z.status for z in rep.zones] == [
        PitStatus.CERTAIN, PitStatus.BOUNDED, PitStatus.UNACHIEVABLE]
    assert rep.query(T0 + timedelta(days=10)).status == PitStatus.CERTAIN
    assert rep.query(T0 + timedelta(days=5)).status == PitStatus.BOUNDED
    assert rep.query(T0 - timedelta(days=1)).status == PitStatus.UNACHIEVABLE
    # Every non-CERTAIN zone names its limiting link (spec §3).
    assert rep.query(T0 + timedelta(days=5)).limiting_chains == ["delta://t"]
    assert rep.query(T0 - timedelta(days=1)).limiting_chains == ["delta://t"]


def test_twice_temporal_conformance():
    wm = _wm("delta://t", T0, T0 + timedelta(days=10))
    good = pit_report("m", [wm],
                      materialization_dt=T0 + timedelta(days=11))
    bad = pit_report("m", [wm],
                     materialization_dt=T0 + timedelta(days=9))
    unknown = pit_report("m", [wm])
    assert good.materialization_conformant is True
    # Built from data that was already partially vacuumed — spec §6 failure.
    assert bad.materialization_conformant is False
    assert unknown.materialization_conformant is None


def test_empty_upstreams_rejected():
    with pytest.raises(ValueError):
        pit_report("m", [])


def test_verdict_two_state():
    wm = _wm("delta://t", T0, T0 + timedelta(days=10))
    assert verdict(wm, since=T0 + timedelta(days=10)).status == VerdictStatus.EXACT
    v = verdict(wm, since=T0 + timedelta(days=5))
    assert v.status == VerdictStatus.BOUNDED
    assert v.limiting_links == ["delta://t"]


def test_parse_dt_normalizes_naive_to_utc():
    assert parse_dt("2026-06-01").tzinfo is timezone.utc
    assert parse_dt("2026-06-01T00:00:00+02:00").utcoffset() == timedelta(hours=2)

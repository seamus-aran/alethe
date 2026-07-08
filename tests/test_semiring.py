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

"""Spec §3: the observability semiring and taint propagation."""

from alethe import K, KRelation, TemporalTable, split_result, verify_semiring_laws


def test_all_semiring_laws_hold():
    assert verify_semiring_laws() == []


def test_ordering_and_identities():
    assert K.ABSENT < K.BEYOND < K.OBSERVED
    # ⊕ = max: any OBSERVED derivation path wins
    r = KRelation(("k",))
    r.add(("x",), K.BEYOND)
    r.add(("x",), K.OBSERVED)
    assert r.data[("x",)] == K.OBSERVED
    # ABSENT rows are never materialized
    r.add(("y",), K.ABSENT)
    assert ("y",) not in r.data


def test_beyond_taints_joins_and_survives_projection():
    orders = KRelation(("cust", "amount"))
    orders.add(("c1", 100), K.OBSERVED)
    orders.add(("c2", 200), K.BEYOND)      # vacuumed source row

    custs = KRelation(("cust", "segment"))
    custs.add(("c1", "smb"), K.OBSERVED)
    custs.add(("c2", "ent"), K.OBSERVED)

    joined = orders.join(custs, on=["cust"])
    # ⊗ = min: the join is only as knowable as its least knowable conjunct
    assert joined.data[("c1", 100, "smb")] == K.OBSERVED
    assert joined.data[("c2", 200, "ent")] == K.BEYOND

    projected = joined.project(("segment",))
    assert projected.data[("ent",)] == K.BEYOND  # taint survives projection

    result = split_result(projected)
    assert result.refused
    assert ("smb",) in result.answers.data
    assert ("ent",) in result.refusals.data


def test_temporal_table_as_of_watermark():
    t = TemporalTable("dim", ("id", "state"), ("id",), retention_watermark=10)
    t.insert_version(("a", "old"), valid_from=1, valid_to=5)
    t.insert_version(("a", "new"), valid_from=5)

    inside = t.as_of(12)
    assert inside.data == {("a", "new"): K.OBSERVED}

    # Below the watermark: known row-shapes come back as BEYOND candidates,
    # values as-recorded — annotation, not value masking (spec §3).
    beyond = t.as_of(3)
    assert set(beyond.data.values()) == {K.BEYOND}
    assert ("a", "old") in beyond.data

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

"""Spec §4: hash-chained manifest integrity and watermark persistence."""

import json
from datetime import datetime, timezone

import pytest

from alethe import Manifest, Watermark, load_watermarks, record
from alethe._models import EvidenceGrade


def _wm(chain: str = "delta://orders") -> Watermark:
    return Watermark(
        chain=chain,
        boundary={"version": 3},
        boundary_dt=datetime(2026, 6, 3, tzinfo=timezone.utc),
        earliest_dt=datetime(2026, 6, 1, tzinfo=timezone.utc),
        evidence_grade=EvidenceGrade.DERIVED,
        empirically_validated=True,
        proof={"readable_at_boundary": True},
    )


def test_append_chains_and_verifies(tmp_path):
    m = Manifest(tmp_path / "m.jsonl")
    e1 = m.append("watermark", chain="a")
    e2 = m.append("watermark", chain="b")
    assert e1["prev_hash"] == "GENESIS"
    assert e2["prev_hash"] == e1["hash"]
    assert m.verify()
    assert Manifest(tmp_path / "m.jsonl").verify()  # survives reload


def test_tampered_entry_breaks_chain_and_refuses_to_load(tmp_path):
    path = tmp_path / "m.jsonl"
    record(_wm(), path)
    lines = path.read_text().splitlines()
    entry = json.loads(lines[0])
    entry["boundary"] = {"version": 999}          # edit history in place
    path.write_text(json.dumps(entry))
    assert not Manifest(path).verify()
    with pytest.raises(ValueError, match="hash-chain verification"):
        load_watermarks(path)


def test_watermark_roundtrip_through_manifest(tmp_path):
    wm = _wm()
    path = tmp_path / "m.jsonl"
    record(wm, path)
    loaded = load_watermarks(path)[wm.chain]
    assert loaded == wm


def test_to_dict_from_dict_inverse():
    wm = _wm()
    assert Watermark.from_dict(wm.to_dict()) == wm


def test_manifest_instance_reuse_single_writer(tmp_path):
    m = Manifest(tmp_path / "m.jsonl")
    record(_wm("delta://a"), m)
    record(_wm("delta://b"), m)
    assert len(m.entries) == 2 and m.verify()
    assert set(load_watermarks(m)) == {"delta://a", "delta://b"}


def test_latest_entry_per_chain_wins(tmp_path):
    path = tmp_path / "m.jsonl"
    old = _wm()
    new = _wm()
    new.boundary_dt = datetime(2026, 6, 4, tzinfo=timezone.utc)
    record(old, path)
    record(new, path)
    assert load_watermarks(path)["delta://orders"].boundary_dt == new.boundary_dt


def test_pre_boundary_dt_entries_are_rejected(tmp_path):
    path = tmp_path / "m.jsonl"
    m = Manifest(path)
    m.append("watermark", chain="delta://legacy", boundary={"version": 1})
    with pytest.raises(ValueError, match="boundary_dt"):
        load_watermarks(path)

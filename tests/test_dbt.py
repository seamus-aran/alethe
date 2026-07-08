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

"""dbt lineage, PIT rewriting, and OpenLineage — against a synthetic
manifest shaped like real dbt output (source → staging → mart + snapshot)."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from alethe import PitStatus, UnachievableQueryError
from alethe._models import EvidenceGrade, Watermark
from alethe.integrations import from_facet, to_facet
from alethe.integrations.dbt import DbtLineage

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)

MANIFEST = {
    "metadata": {"dbt_schema_version":
                 "https://schemas.getdbt.com/dbt/manifest/v12.json"},
    "sources": {
        "source.p.raw.orders": {
            "unique_id": "source.p.raw.orders", "resource_type": "source",
            "source_name": "raw", "name": "orders", "schema": "raw",
            "relation_name": "raw.orders",
            "meta": {"alethe": {"chain": "delta://orders"}},
        },
    },
    "nodes": {
        "model.p.stg_orders": {
            "unique_id": "model.p.stg_orders", "resource_type": "model",
            "name": "stg_orders", "path": "staging/stg_orders.sql",
            "depends_on": {"nodes": ["source.p.raw.orders"]},
        },
        "snapshot.p.customers_snapshot": {
            "unique_id": "snapshot.p.customers_snapshot",
            "resource_type": "snapshot", "name": "customers_snapshot",
            "relation_name": "snapshots.customers_snapshot",
            "config": {"strategy": "timestamp",
                       "snapshot_meta_column_names": {}},
        },
        "model.p.mart": {
            "unique_id": "model.p.mart", "resource_type": "model",
            "name": "mart", "path": "marts/mart.sql",
            "depends_on": {"nodes": ["model.p.stg_orders",
                                     "snapshot.p.customers_snapshot",
                                     "macro.p.some_macro"]},
        },
    },
}


def _wm(chain="delta://orders", earliest=T0, boundary=T0 + timedelta(days=10)):
    return Watermark(chain=chain, boundary={"version": 1},
                     boundary_dt=boundary, earliest_dt=earliest,
                     evidence_grade=EvidenceGrade.DERIVED,
                     empirically_validated=True, proof={})


@pytest.fixture()
def lineage(tmp_path):
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(MANIFEST))
    return DbtLineage(p)


def test_bfs_finds_leaves_and_skips_macros(lineage):
    leaves = {n["unique_id"] for n in lineage.upstream_leaves("mart")}
    assert leaves == {"source.p.raw.orders", "snapshot.p.customers_snapshot"}
    # memoized BFS returns equal content on fresh copies
    again = lineage.upstream_leaves("mart")
    assert {n["unique_id"] for n in again} == leaves
    assert again is not lineage.upstream_leaves("mart")


def test_resolve_watermarks_via_meta_and_name_fallback(lineage):
    chains = {"delta://orders": _wm(),
              "snapshot://customers_snapshot": _wm("snapshot://customers_snapshot")}
    resolved = lineage.resolve_watermarks("mart", chains)
    assert resolved["source.p.raw.orders"].chain == "delta://orders"       # meta
    assert (resolved["snapshot.p.customers_snapshot"].chain
            == "snapshot://customers_snapshot")                            # name


def test_pit_report_composes_upstreams(lineage):
    wms = {"source.p.raw.orders": _wm(),
           "snapshot.p.customers_snapshot":
               _wm("snapshot://customers_snapshot",
                   boundary=T0 + timedelta(days=20))}
    rep = lineage.pit_report("mart", watermarks=wms)
    assert rep.limiting_chain == "snapshot://customers_snapshot"
    assert rep.query(T0 + timedelta(days=15)).status == PitStatus.BOUNDED


def test_rewrite_binds_source_and_snapshot(lineage):
    wms = {"source.p.raw.orders": _wm(),
           "snapshot.p.customers_snapshot":
               _wm("snapshot://customers_snapshot",
                   boundary=T0 + timedelta(days=20))}
    sql = ("WITH o AS (SELECT * FROM raw.orders) "
           "SELECT o.id, c.segment FROM o "
           "JOIN snapshots.customers_snapshot c ON o.cust = c.id")
    res = lineage.rewrite_model(
        "mart", T0 + timedelta(days=15), watermarks=wms, compiled_sql=sql)
    # Physical source: storage time travel. Snapshot: SCD2 validity
    # predicate — its history lives in rows, not versions.
    assert "raw.orders" in res.bound_tables
    assert "TIMESTAMP AS OF" in res.sql
    assert "dbt_valid_from" in res.sql and "dbt_valid_to" in res.sql
    assert res.status == PitStatus.BOUNDED and res.warnings


def test_rewrite_refuses_unachievable(lineage):
    wms = {"source.p.raw.orders": _wm(),
           "snapshot.p.customers_snapshot": _wm("snapshot://customers_snapshot")}
    with pytest.raises(UnachievableQueryError):
        lineage.rewrite_model("mart", T0 - timedelta(days=1),
                              watermarks=wms,
                              compiled_sql="SELECT * FROM raw.orders")


def test_ambiguous_and_missing_names(lineage):
    with pytest.raises(KeyError):
        lineage.upstream_leaves("no_such_model")


def test_dbt_macro_copies_do_not_drift():
    root = Path(__file__).resolve().parents[1]
    canonical = root / "dbt_macros" / "alethe_pit.sql"
    poc_copy = root / "poc" / "dbt" / "project" / "macros" / "alethe_pit.sql"
    if not poc_copy.exists():
        pytest.skip("POC macro copy not present")
    assert canonical.read_text() == poc_copy.read_text(), (
        "dbt_macros/alethe_pit.sql and the POC copy have drifted — "
        "edit dbt_macros/ and copy to the POC, not the other way around.")


def test_openlineage_facet_roundtrip():
    wm = _wm()
    facet = to_facet(wm)
    assert "observabilityWatermark" in facet
    assert from_facet(wm.chain, facet) == wm
    # naive facet timestamps are normalized to UTC on the way in
    facet["observabilityWatermark"]["boundaryTime"] = "2026-01-11T00:00:00"
    assert from_facet(wm.chain, facet).boundary_dt.tzinfo is not None

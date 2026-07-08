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

"""alethe.asof() and the adapters, against real vacuumed Delta tables."""

from datetime import timedelta

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("sqlglot")

import alethe  # noqa: E402
from alethe import UnachievableQueryError  # noqa: E402
from alethe._asof import _write_pattern  # noqa: E402
from alethe.integrations import epistemic_view_sql, lower_bound_sql  # noqa: E402


def _fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


@pytest.fixture(scope="module")
def orders_wm(vacuumed_orders):
    return alethe.watermark(vacuumed_orders)


def test_watermark_is_empirically_validated(orders_wm):
    assert orders_wm.empirically_validated
    assert orders_wm.earliest_dt < orders_wm.boundary_dt
    assert orders_wm.chain.startswith("delta://")


def test_naive_time_travel_lies(vacuumed_orders, orders_wm):
    """The engine resolves a vacuumed timestamp at plan time and only
    fails (or worse, silently misleads) at read time — the failure mode
    OWS exists to prevent."""
    from deltalake import DeltaTable
    dt = DeltaTable(vacuumed_orders)
    dt.load_as_version(orders_wm.earliest_dt + timedelta(milliseconds=1))
    with pytest.raises(Exception):
        dt.to_pyarrow_table()  # files are physically gone


def test_asof_exact(vacuumed_orders, orders_wm):
    ts = _fmt(orders_wm.boundary_dt + timedelta(seconds=1))
    res = alethe.asof(
        f"SELECT order_date, SUM(amount) AS revenue FROM orders "
        f"TIMESTAMP AS OF '{ts}' GROUP BY 1",
        tables={"orders": vacuumed_orders})
    assert res.status == "EXACT" and not res.notes
    assert len(res.df) == 3  # days 3-5 survive the retention delete
    assert str(res.df["revenue"].dtype) == "float64"  # typed, not stringified


def test_asof_bounded_is_labelled_lower_bound(vacuumed_orders, orders_wm):
    ts = _fmt(orders_wm.earliest_dt + timedelta(milliseconds=1))
    res = alethe.asof(
        f"SELECT SUM(amount) AS revenue FROM orders TIMESTAMP AS OF '{ts}'",
        tables={"orders": vacuumed_orders})
    assert res.status == "BOUNDED"
    assert "LOWER BOUNDS" in res.notes[0]  # append pattern: floor is valid


def test_asof_unachievable_refuses(vacuumed_orders, orders_wm):
    ts = _fmt(orders_wm.earliest_dt - timedelta(days=1))
    with pytest.raises(UnachievableQueryError):
        alethe.asof(f"SELECT * FROM orders TIMESTAMP AS OF '{ts}'",
                    tables={"orders": vacuumed_orders})


def test_asof_self_join_rules(vacuumed_orders, orders_wm):
    ok = _fmt(orders_wm.boundary_dt + timedelta(seconds=1))
    other = _fmt(orders_wm.boundary_dt + timedelta(seconds=2))
    res = alethe.asof(
        f"SELECT a.order_date FROM orders TIMESTAMP AS OF '{ok}' a "
        f"JOIN orders TIMESTAMP AS OF '{ok}' b ON a.order_id = b.order_id",
        tables={"orders": vacuumed_orders})
    assert res.status == "EXACT"
    with pytest.raises(ValueError, match="different AS OF"):
        alethe.asof(
            f"SELECT a.order_date FROM orders TIMESTAMP AS OF '{ok}' a "
            f"JOIN orders TIMESTAMP AS OF '{other}' b ON a.order_id = b.order_id",
            tables={"orders": vacuumed_orders})


def test_asof_rejects_non_delta(tmp_path, orders_wm):
    ts = _fmt(orders_wm.boundary_dt)
    with pytest.raises(NotImplementedError, match="Delta"):
        alethe.asof(f"SELECT * FROM t TIMESTAMP AS OF '{ts}'",
                    tables={"t": tmp_path})


def test_write_pattern_detection(vacuumed_orders, vacuumed_balances):
    from deltalake import DeltaTable
    assert _write_pattern(DeltaTable(vacuumed_orders)) == \
        "append-with-retention-delete"
    assert _write_pattern(DeltaTable(vacuumed_balances)) == "overwrite"


def test_asof_overwrite_is_temporal_substitution(vacuumed_balances):
    wm = alethe.watermark(vacuumed_balances)
    if wm.earliest_dt >= wm.boundary_dt:
        pytest.skip("vacuum left no BOUNDED window on this run")
    ts = _fmt(wm.earliest_dt + timedelta(milliseconds=1))
    res = alethe.asof(
        f"SELECT * FROM balances TIMESTAMP AS OF '{ts}'",
        tables={"balances": vacuumed_balances})
    assert res.status == "BOUNDED"
    # An overwritten state is NOT a subset of the true state — no floor.
    assert "TEMPORAL SUBSTITUTION" in res.notes[0]
    assert "LOWER BOUNDS" not in res.notes[0]


def test_epistemic_view_contract():
    """Annotation, not value masking: measures keep SQL types with NULL
    where unknowable; a separate column separates BEYOND (destroyed)
    from ABSENT (a real zero) at the boundary."""
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE observed AS SELECT DATE '2026-06-05' AS d, 10.0 AS amt "
                "UNION ALL SELECT DATE '2026-06-06', 20.0")
    con.execute("CREATE TABLE spine AS SELECT UNNEST(generate_series("
                "DATE '2026-06-03', DATE '2026-06-07', INTERVAL 1 DAY))::DATE AS d")
    view = epistemic_view_sql(
        observed="SELECT * FROM observed", spine="SELECT * FROM spine",
        key="d", boundary="DATE '2026-06-05'", measures=["amt"])
    rows = dict(con.execute(
        f"SELECT d, epistemic FROM ({view}) ORDER BY d").fetchall())
    statuses = {str(k): v for k, v in rows.items()}
    assert statuses["2026-06-03"] == "BEYOND"    # destroyed by retention
    assert statuses["2026-06-04"] == "BEYOND"
    assert statuses["2026-06-05"] == "OBSERVED"
    assert statuses["2026-06-07"] == "ABSENT"    # inside retention: real zero

    floor_sql = lower_bound_sql(view=view, measure="amt")
    floor, is_lb, unknowable = con.execute(floor_sql).fetchone()
    assert (floor, is_lb, unknowable) == (30.0, True, 2)
    con.close()

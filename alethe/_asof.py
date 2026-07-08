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

"""The analyst one-liner: run a time-travel query, get an honest result.

    import alethe

    res = alethe.asof(
        "SELECT segment, SUM(amount) FROM orders TIMESTAMP AS OF '2026-06-09' "
        "GROUP BY segment",
        tables={"orders": "path/to/delta/orders"},
    )
    res.df        # typed pandas DataFrame
    res.status    # 'EXACT' or 'BOUNDED'
    res           # in a notebook: renders banner + table

Per referenced table, the requested time is classified against its
empirically validated watermark:

- CERTAIN       → real time travel to the requested timestamp
- BOUNDED       → the read is clamped to the boundary (the earliest
                  surviving state) and the result is labelled — including
                  whether that is a lower bound (append-pattern tables)
                  or a temporal substitution (overwrite-pattern tables),
                  detected from the table's own transaction log
- UNACHIEVABLE  → UnachievableQueryError; no honest answer exists

Requires: pip install alethe[asof]   (sqlglot + duckdb)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ._models import Watermark


@dataclass
class AsOfResult:
    df: "object"                     # pandas DataFrame, engine types intact
    status: str                      # 'EXACT' | 'BOUNDED'
    requested: datetime
    notes: list[str]                 # one plain-English line per limitation
    watermarks: dict[str, Watermark] = field(default_factory=dict)

    def _banner(self) -> str:
        if self.status == "EXACT":
            return "✓ EXACT — fully inside retention; this is the true state."
        return "⚠ BOUNDED — " + " ".join(self.notes)

    def __repr__(self) -> str:
        return f"{self._banner()}\n\n{self.df.to_string(index=False)}"

    def _repr_html_(self) -> str:
        color = "#2e7d32" if self.status == "EXACT" else "#ef6c00"
        notes = "".join(f"<div style='color:#555;font-size:0.9em'>• {n}</div>"
                        for n in self.notes)
        return (f"<div style='border-left:4px solid {color};"
                f"padding:6px 12px;margin:4px 0;background:#fafafa'>"
                f"<b style='color:{color}'>{self._banner().splitlines()[0]}</b>"
                f"{notes}</div>" + self.df.to_html(index=False))


def _write_pattern(dt) -> str:
    """'overwrite' if any commit replaced table state, else 'append'
    (retention DELETEs keep surviving rows a subset of the true rows, so
    the lower-bound reading still holds). Decides whether a clamped
    BOUNDED read is a lower bound or a temporal substitution. Uses the
    sanctioned ``DeltaTable.history()`` API, never the raw log."""
    saw_delete = False
    for commit in dt.history():
        op = commit.get("operation")
        params = commit.get("operationParameters") or {}
        mode = str(params.get("mode") or "").strip('"')
        if op == "WRITE" and mode not in ("", "Append"):
            return "overwrite"
        if op == "DELETE":
            saw_delete = True
    return "append-with-retention-delete" if saw_delete else "append"


def asof(sql: str, *, tables: dict[str, str | Path],
         dialect: str = "spark") -> AsOfResult:
    """Execute a time-travel SQL query with zone gating and honest labels.

    Parameters
    ----------
    sql:
        A query using engine-native time travel (``TIMESTAMP AS OF '...'``
        on one or more tables). Joins are supported; each tracked table
        is gated and read independently.
    tables:
        Map of table name (as written in the SQL) → Delta table path.
    dialect:
        sqlglot dialect the SQL is written in (default ``"spark"``).
    """
    try:
        import sqlglot
        from sqlglot import exp
        import duckdb
    except ImportError as e:
        raise ImportError(
            "alethe.asof requires sqlglot and duckdb: "
            "pip install alethe[asof]") from e
    from deltalake import DeltaTable
    from . import watermark as _watermark, pit_report
    from ._models import PitStatus, VerdictStatus
    from .integrations.pit_rewriter import UnachievableQueryError

    tree = sqlglot.parse_one(sql, read=dialect)

    requested: datetime | None = None
    notes: list[str] = []
    wms: dict[str, Watermark] = {}
    frames: dict[str, object] = {}
    seen_ts: dict[str, datetime] = {}
    worst = VerdictStatus.EXACT

    for tnode in tree.find_all(exp.Table):
        name = tnode.name
        if name not in tables:
            continue
        version = tnode.args.get("version")
        if version is None:
            continue
        ts = datetime.fromisoformat(version.expression.name)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        tnode.set("version", None)          # strip AS OF for local execution

        if name in seen_ts:                 # self-join: gate + load once
            if ts != seen_ts[name]:
                raise ValueError(
                    f"{name!r} is referenced with two different AS OF "
                    f"timestamps ({seen_ts[name].isoformat()} and "
                    f"{ts.isoformat()}); one state per table per query.")
            continue
        seen_ts[name] = ts
        requested = requested or ts

        path = Path(tables[name])
        if not (path / "_delta_log").exists():
            raise NotImplementedError(
                f"asof() currently supports Delta tables only; {name!r} at "
                f"{path} has no _delta_log/. For Iceberg, derive the "
                "watermark with alethe.watermark(..., adapter='iceberg') "
                "and gate with integrations.rewrite_pit().")
        wm = _watermark(path)
        wms[wm.chain] = wm
        zone = pit_report(name, [wm]).query(ts)

        if zone.status == PitStatus.UNACHIEVABLE:
            raise UnachievableQueryError(
                f"AS OF {ts.isoformat()} precedes the existence of "
                f"{wm.chain}. No honest answer exists — the population "
                "itself is unknowable at that time.")

        dt = DeltaTable(str(path))
        if zone.status == PitStatus.BOUNDED:
            worst = VerdictStatus.BOUNDED
            pattern = _write_pattern(dt)
            dt.load_as_version(wm.boundary["version"])
            if pattern.startswith("append"):
                notes.append(
                    f"{name}: retention destroyed history before "
                    f"{wm.boundary_dt.strftime('%Y-%m-%d %H:%M:%S')}; rows "
                    "shown are the surviving evidence — totals over "
                    "non-negative measures are LOWER BOUNDS (append-only "
                    "table: missing rows could only add).")
            else:
                notes.append(
                    f"{name}: retention destroyed the state at the requested "
                    f"time; showing the earliest surviving state "
                    f"({wm.boundary_dt.strftime('%Y-%m-%d %H:%M:%S')}) instead — "
                    "a TEMPORAL SUBSTITUTION, not a bound (overwrite table: "
                    "values may have been higher or lower).")
        else:
            dt.load_as_version(ts)          # real time travel

        frames[name] = dt.to_pyarrow_table()

    if requested is None:
        raise ValueError(
            "No 'TIMESTAMP AS OF' found on any table in `tables`. "
            "Write the query with engine-native time travel syntax.")

    con = duckdb.connect()
    for name, frame in frames.items():
        con.register(name, frame)
    df = con.execute(tree.sql(dialect="duckdb")).fetch_df()
    con.close()

    return AsOfResult(df=df, status=worst.value, requested=requested,
                      notes=notes, watermarks=wms)

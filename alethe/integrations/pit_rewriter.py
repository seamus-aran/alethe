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

"""Point-in-time SQL rewriting.

Rewrites a query so that every reference to a tracked source table is
bound to a specific point in time with the engine's native time-travel
syntax — `TIMESTAMP AS OF` (Spark/Delta), `FOR TIMESTAMP AS OF` (Trino/
Iceberg), `FOR SYSTEM_TIME AS OF` (BigQuery).

Only *physical* source tables are bound.  CTEs and subquery aliases are
left untouched: binding the sources is sufficient, because everything
downstream of a bound source derives from it, and binding intermediate
CTEs would be redundant (and, for engines that reject time-travel on
non-table relations, wrong).

The rewrite is refused when the requested point in time falls in the
UNACHIEVABLE zone of the model's PIT report — rewriting a query to a
time before the source existed would produce a silently empty (falsely
confident) answer, which is exactly what OWS exists to prevent.

Requires: pip install alethe[rewrite]  (adds sqlglot)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime

from .._models import PitReport, PitStatus


class UnachievableQueryError(Exception):
    """The requested point in time precedes the existence of at least one
    upstream source; no rewrite can produce an honest answer."""


@dataclass
class RewriteResult:
    sql: str                       # the rewritten query
    since: datetime                # the bound point in time
    dialect: str
    bound_tables: list[str]        # tables that received a time-travel clause
    unmatched_tracked: list[str]   # tracked tables not found in the SQL
    report: PitReport | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def status(self) -> PitStatus | None:
        return self.report.query(self.since).status if self.report else None


def rewrite_pit(
    sql: str,
    since: datetime,
    tracked_tables: list[str],
    *,
    dialect: str = "spark",
    report: PitReport | None = None,
) -> RewriteResult:
    """Bind every reference to a tracked table to ``since``.

    Parameters
    ----------
    sql:
        The query to rewrite (compiled SQL — no Jinja).
    since:
        Timezone-aware point in time to bind to.
    tracked_tables:
        Table identifiers to bind.  Matching is suffix-based on dotted
        parts, so ``"raw.orders"`` matches ``catalog.raw.orders`` and
        ``raw.orders`` but not ``other.orders``.
    dialect:
        sqlglot dialect for both parsing and rendering:
        ``"spark"``, ``"trino"``, ``"bigquery"``, ``"duckdb"``, ...
    report:
        Optional PIT report for the model.  When given, a ``since`` in
        the UNACHIEVABLE zone raises :class:`UnachievableQueryError`,
        and a ``since`` in the BOUNDED zone attaches a warning.

    Returns
    -------
    RewriteResult with the rewritten SQL and binding details.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError as e:
        raise ImportError(
            "PIT rewriting requires sqlglot: pip install alethe[rewrite]") from e

    warnings: list[str] = []
    if report is not None:
        zone = report.query(since)
        if zone.status == PitStatus.UNACHIEVABLE:
            raise UnachievableQueryError(
                f"AS OF {since.isoformat()} precedes the existence of "
                f"{zone.limiting_chains}. No honest rewrite is possible — "
                "the population itself is unknowable at that time (spec §9).")
        if zone.status == PitStatus.BOUNDED:
            warnings.append(
                f"AS OF {since.isoformat()} is inside the BOUNDED zone: "
                f"retention has destroyed part of {zone.limiting_chains}. "
                "Monotone aggregates are lower bounds; non-monotone queries "
                "(NOT EXISTS, MIN/MAX) should be REFUSED.")

    tree = sqlglot.parse_one(sql, read=dialect)

    # Collect CTE alias names — references to these are not physical tables.
    cte_names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}

    tracked_parts = [tuple(t.lower().split(".")) for t in tracked_tables]

    ts_literal = exp.Cast(
        this=exp.Literal.string(since.strftime("%Y-%m-%d %H:%M:%S")),
        to=exp.DataType.build("timestamp"),
    )

    bound: list[str] = []
    for table in tree.find_all(exp.Table):
        name_parts = tuple(
            p for p in (table.catalog, table.db, table.name) if p)
        if not name_parts:
            continue
        if len(name_parts) == 1 and name_parts[0] in cte_names:
            continue  # CTE reference, not a physical table
        lowered = tuple(p.lower() for p in name_parts)
        if not any(lowered[-len(t):] == t for t in tracked_parts
                   if len(t) <= len(lowered)):
            continue
        table.set("version", exp.Version(
            this="TIMESTAMP", expression=ts_literal.copy(), kind="AS OF"))
        bound.append(".".join(name_parts))

    matched = {tuple(t.lower().split(".")) for t in tracked_tables
               if any(tuple(p.lower() for p in b.split("."))[-len(t.split(".")):]
                      == tuple(t.lower().split(".")) for b in bound)}
    unmatched = [t for t in tracked_tables
                 if tuple(t.lower().split(".")) not in matched]
    if unmatched:
        warnings.append(
            f"Tracked tables not referenced in this query: {unmatched}. "
            "If the model reads them through another path, that path is "
            "not bound.")

    return RewriteResult(
        sql=tree.sql(dialect=dialect, pretty=True),
        since=since,
        dialect=dialect,
        bound_tables=bound,
        unmatched_tracked=unmatched,
        report=report,
        warnings=warnings,
    )

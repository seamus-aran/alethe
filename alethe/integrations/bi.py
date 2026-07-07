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

"""BI-facing presentation of BOUNDED results — as SQL, with intact types.

The spec's rule for row-level epistemic status is *annotation, not value
masking*: measures keep their SQL types (NULL when unknowable) and the
status travels in a separate column. That keeps results consumable by any
BI tool — the epistemic column is an ordinary dimension for filtering and
conditional formatting, and the measures stay aggregable:

    NULL value + epistemic = 'OBSERVED'  → a genuine null in the data
    NULL value + epistemic = 'BEYOND'    → evidence destroyed by retention
    epistemic = 'ABSENT'                 → the key exists in the spine but
                                           no fact was ever recorded there
                                           (inside retention — a real zero)

`epistemic_view_sql` renders that contract as one SELECT: a spine (the
known population — a calendar, a dimension) LEFT JOINed to the observed
facts, with the CASE that separates BEYOND from ABSENT at the watermark
boundary. Register it as a view and point the BI tool at it.
"""

from __future__ import annotations


def epistemic_view_sql(
    *,
    observed: str,
    spine: str,
    key: str,
    boundary: str,
    measures: list[str],
    epistemic_column: str = "epistemic",
) -> str:
    """Render a typed, BI-consumable epistemic view as a single SELECT.

    Parameters
    ----------
    observed:
        SQL for the observed facts (a table name or a parenthesizable
        SELECT), exposing ``key`` plus the ``measures`` columns. Rows
        here are the surviving, readable evidence.
    spine:
        SQL for the known population of keys (a calendar table, a
        dimension) exposing ``key``. This is what makes destroyed rows
        *presentable*: the spine says they existed, the boundary says
        their evidence is gone.
    key:
        The join column, e.g. ``"order_date"``.
    boundary:
        SQL expression for the observability boundary in ``key``'s
        domain, e.g. ``"DATE '2026-06-05'"``. Spine keys below it with
        no observed row are BEYOND; at/above it they are ABSENT.
    measures:
        Measure column names carried through from ``observed``. They
        keep their SQL types; BEYOND/ABSENT rows carry NULL.
    epistemic_column:
        Name of the status column (default ``"epistemic"``).

    Returns
    -------
    A SELECT statement. Wrap it in ``CREATE VIEW ... AS`` or use it as a
    subquery. Works on any SQL engine — it is plain ANSI SQL.
    """
    measure_cols = ",\n       ".join(f"o.{m}" for m in measures)
    return f"""\
SELECT s.{key},
       {measure_cols},
       CASE
         WHEN o.{key} IS NOT NULL THEN 'OBSERVED'
         WHEN s.{key} < {boundary} THEN 'BEYOND'
         ELSE 'ABSENT'
       END AS {epistemic_column}
FROM ({spine}) s
LEFT JOIN ({observed}) o ON s.{key} = o.{key}"""


def lower_bound_sql(
    *,
    view: str,
    measure: str,
    epistemic_column: str = "epistemic",
) -> str:
    """Aggregate an epistemic view into a floor with a bound flag.

    Emits one row: the SUM over observed rows plus ``is_lower_bound`` —
    true when any BEYOND row was in scope, meaning the true total can
    only be higher (valid for non-negative, accumulate-only measures).
    In a BI tool, bind the '≥' prefix / footnote to ``is_lower_bound``.
    """
    return f"""\
SELECT SUM({measure})                                        AS {measure}_floor,
       BOOL_OR({epistemic_column} = 'BEYOND')                AS is_lower_bound,
       COUNT(*) FILTER (WHERE {epistemic_column} = 'BEYOND') AS keys_unknowable
FROM ({view})"""

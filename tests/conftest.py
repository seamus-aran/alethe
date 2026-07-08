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

"""Shared fixtures: real Delta tables with real VACUUM damage.

The tests do not simulate retention — they build actual delta-rs tables,
destroy history with a real DELETE + VACUUM, and assert against what the
engine can and cannot physically read. That mirrors the spec's rule that
boundaries are validated empirically, never by metadata arithmetic alone.
"""

from __future__ import annotations
import time
from datetime import date

import pytest

deltalake = pytest.importorskip("deltalake")
pa = pytest.importorskip("pyarrow")

from deltalake import DeltaTable, write_deltalake  # noqa: E402


def _orders_batch(day: int) -> "pa.Table":
    return pa.table({
        "order_id": pa.array([day * 10 + i for i in range(3)], pa.int64()),
        "order_date": pa.array([date(2026, 6, day)] * 3, pa.date32()),
        "amount": pa.array([100.0 * day + i for i in range(3)], pa.float64()),
    })


@pytest.fixture(scope="session")
def vacuumed_orders(tmp_path_factory) -> str:
    """Append-pattern table: 5 daily appends, then a retention DELETE of
    the first two days followed by VACUUM.  Versions before the delete
    are listed in the log but physically unreadable."""
    path = str(tmp_path_factory.mktemp("delta") / "orders")
    for day in range(1, 6):
        write_deltalake(path, _orders_batch(day), mode="append")
        time.sleep(0.05)  # distinct commit timestamps (ms precision)
    dt = DeltaTable(path)
    dt.delete("order_date < '2026-06-03'")
    time.sleep(0.05)
    dt.vacuum(retention_hours=0, dry_run=False,
              enforce_retention_duration=False)
    return path


@pytest.fixture(scope="session")
def vacuumed_balances(tmp_path_factory) -> str:
    """Overwrite-pattern table: 3 full overwrites, then VACUUM.  A clamped
    read of this table is a temporal substitution, never a lower bound."""
    path = str(tmp_path_factory.mktemp("delta") / "balances")
    for version in range(3):
        frame = pa.table({
            "account": pa.array(["a", "b"], pa.string()),
            "balance": pa.array([10.0 + version, 20.0 + version], pa.float64()),
        })
        write_deltalake(path, frame, mode="overwrite")
        time.sleep(0.05)
    DeltaTable(path).vacuum(retention_hours=0, dry_run=False,
                            enforce_retention_duration=False)
    return path

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

"""
Alethe Phase 1 — The Watermark Oracle, against a REAL Delta table
=================================================================
No simulation. This script:

  1. Creates an actual Delta table on disk (delta-rs) and writes 8
     versions over a simulated month of activity.
  2. Runs a real VACUUM that physically deletes unreferenced parquet.
  3. ORACLE PHASE A (metadata derivation): parses the raw _delta_log
     JSON commits, reconstructs the exact file set of every version by
     replaying add/remove actions, checks physical file existence, and
     derives the candidate boundary = earliest version whose full file
     set still exists.
  4. ORACLE PHASE B (empirical validation, the OWS conformance
     requirement): attempts a REAL time-travel read at every version.
     The boundary is confirmed iff reads at/after it succeed and reads
     before it fail. Metadata arithmetic is never trusted alone.
  5. Emits the watermark tuple (boundary, grade, proof) and demonstrates
     the headline dishonesty: the log still LISTS versions that are
     physically unreadable.
"""

from __future__ import annotations
import json
import os
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake

TABLE = Path(__file__).parent / "lakehouse" / "sales_orders"


# ---------------------------------------------------------------------------
# Step 1+2: build a real table with history, then really vacuum it
# ---------------------------------------------------------------------------

def build_table() -> None:
    TABLE.parent.mkdir(parents=True, exist_ok=True)
    for v in range(8):
        df = pd.DataFrame({
            "order_id": [f"O{v}{i}" for i in range(5)],
            "customer_id": [f"C{i % 3}" for i in range(5)],
            "amount": [100.0 * (v + 1) + i for i in range(5)],
            "day": [v * 4] * 5,
        })
        # overwrite each time so prior files become unreferenced
        write_deltalake(TABLE, df, mode="overwrite")
    print(f"  Wrote 8 real versions to {TABLE}")


def vacuum_table() -> list[str]:
    dt = DeltaTable(str(TABLE))
    removed = dt.vacuum(retention_hours=0,
                        enforce_retention_duration=False,
                        dry_run=False)
    print(f"  REAL VACUUM removed {len(removed)} parquet file(s).")
    return removed


# ---------------------------------------------------------------------------
# Step 3: Phase A — derive the boundary from the raw _delta_log
# ---------------------------------------------------------------------------

def replay_log(table: Path) -> dict[int, set[str]]:
    """Reconstruct each version's full file set by replaying add/remove
    actions from the commit JSONs. Returns {version: {relative paths}}."""
    log_dir = table / "_delta_log"
    commits = sorted(p for p in log_dir.glob("*.json"))
    live: set[str] = set()
    per_version: dict[int, set[str]] = {}
    for commit in commits:
        version = int(commit.stem)
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:
                live.add(action["add"]["path"])
            elif "remove" in action:
                live.discard(action["remove"]["path"])
        per_version[version] = set(live)
    return per_version


def derive_boundary(table: Path) -> tuple[int | None, dict[int, dict]]:
    per_version = replay_log(table)
    detail: dict[int, dict] = {}
    candidate = None
    for v in sorted(per_version):
        files = per_version[v]
        missing = {f for f in files if not (table / f).exists()}
        detail[v] = {"files": len(files), "missing": len(missing)}
        if not missing and candidate is None:
            candidate = v
    return candidate, detail


# ---------------------------------------------------------------------------
# Step 4: Phase B — empirical validation with real time-travel reads
# ---------------------------------------------------------------------------

def try_read(table: Path, version: int) -> tuple[bool, str]:
    try:
        dt = DeltaTable(str(table), version=version)
        n = dt.to_pyarrow_table().num_rows   # force real file reads
        return True, f"read {n} rows"
    except Exception as e:
        return False, type(e).__name__


def empirically_validate(table: Path, candidate: int,
                         versions: list[int]) -> tuple[bool, dict[int, str]]:
    results: dict[int, str] = {}
    ok = True
    for v in versions:
        readable, note = try_read(table, v)
        results[v] = f"{'READABLE' if readable else 'UNREADABLE'} ({note})"
        if v >= candidate and not readable:
            ok = False      # boundary claims readability it can't deliver
        if v < candidate and readable:
            ok = False      # boundary is too conservative — also a defect
    return ok, results


# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 74)
    print("  ALETHE WATERMARK ORACLE — real _delta_log, real vacuum, real reads")
    print("=" * 74)

    print("\n[1] Build table")
    build_table()

    print("\n[2] Vacuum (retention=0h, enforcement disabled — the footgun")
    print("    every engine exposes and OWS exists to make honest)")
    vacuum_table()

    print("\n[3] Phase A — metadata derivation from raw _delta_log")
    candidate, detail = derive_boundary(TABLE)
    dt_now = DeltaTable(str(TABLE))
    listed = [h["version"] for h in dt_now.history()]
    print(f"    Log still lists versions: {sorted(listed)}")
    for v in sorted(detail):
        d = detail[v]
        flag = "  <- candidate boundary" if v == candidate else ""
        print(f"    v{v}: {d['files']} file(s), {d['missing']} missing{flag}")
    print(f"    Derived candidate boundary: version {candidate}")

    print("\n[4] Phase B — empirical validation (real time-travel reads)")
    versions = sorted(detail)
    validated, results = empirically_validate(TABLE, candidate, versions)
    for v in versions:
        print(f"    AS OF version {v}: {results[v]}")
    print(f"    empirically_validated = {validated}")

    print("\n[5] Watermark emitted")
    watermark = {
        "chain": f"delta://{TABLE.name}",
        "boundary": {"version": candidate},
        "evidence_grade": "derived",
        "empirically_validated": validated,
        "proof": {
            "listed_versions": sorted(int(x) for x in listed),
            "readable_versions": [v for v in versions
                                  if results[v].startswith("READABLE")],
        },
    }
    print(json.dumps(watermark, indent=2))

    unreadable = [v for v in versions if results[v].startswith("UNREADABLE")]
    print(f"\n  The dishonesty, measured: the log advertises "
          f"{len(listed)} versions; only "
          f"{len(versions) - len(unreadable)} are readable. "
          f"Versions {unreadable} are ghosts —")
    print("  a naive AS OF against them fails at read time with no warning "
          "at plan time.")
    print("  The oracle turns that into a provable, manifest-ready boundary.")


if __name__ == "__main__":
    main()

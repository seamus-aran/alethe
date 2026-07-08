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


# NOTE: Phase-1 reference implementation, written before the `alethe`
# package existed. Kept as the spec's original evidence artifact.
# The maintained equivalent lives in: alethe.adapters.iceberg + alethe.Manifest / notebooks/01

"""
OWS — Manifest integration + second adapter (Iceberg)
=====================================================
Continues from ows_delta_oracle.py. Two steps:

  STEP 1: Wire the REAL Delta oracle into the hash-chained manifest,
          adding the cross-check: the boundary derived from file
          existence must be corroborated by the VACUUM START/END
          commits the log itself recorded. Two independent derivations
          of the same boundary = stronger evidence than either alone.

  STEP 2: A REAL Apache Iceberg adapter (pyiceberg + sqlite catalog):
          same watermark contract, completely different metadata model
          (snapshot lineage in metadata.json instead of a commit log).
          History is destroyed out-of-band (deleted data files — the
          remove_orphan_files / manual-deletion failure mode), and the
          oracle derives + empirically validates the boundary from
          snapshot metadata and real reads.

One contract, two formats. That is the generalization claim, executed.
"""

from __future__ import annotations
import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa

# ---------------------------------------------------------------------------
# The manifest (same hash-chain mechanics as ows_examples.py)
# ---------------------------------------------------------------------------

class Manifest:
    def __init__(self, path: Path):
        self.path = path
        self.entries: list[dict] = []
        if path.exists():
            self.entries = [json.loads(l) for l in path.read_text().splitlines()]

    def append(self, kind: str, **payload) -> dict:
        prev_hash = self.entries[-1]["hash"] if self.entries else "GENESIS"
        body = {"seq": len(self.entries), "kind": kind,
                "prev_hash": prev_hash, **payload}
        body["hash"] = hashlib.sha256(json.dumps(
            body, sort_keys=True, default=str).encode()).hexdigest()[:16]
        self.entries.append(body)
        self.path.write_text("\n".join(
            json.dumps(e, default=str) for e in self.entries))
        return body

    def verify(self) -> bool:
        prev = "GENESIS"
        for e in self.entries:
            if e["prev_hash"] != prev:
                return False
            body = {k: v for k, v in e.items() if k != "hash"}
            if hashlib.sha256(json.dumps(body, sort_keys=True,
                                         default=str).encode()
                              ).hexdigest()[:16] != e["hash"]:
                return False
            prev = e["hash"]
        return True


# ===========================================================================
# STEP 1 — Delta oracle -> manifest, with vacuum-commit cross-check
# ===========================================================================

def delta_watermark_to_manifest(table: Path, manifest: Manifest) -> dict:
    from deltalake import DeltaTable

    # --- derivation A: file-existence replay (from ows_delta_oracle) ---
    log_dir = table / "_delta_log"
    live: set[str] = set()
    per_version: dict[int, set[str]] = {}
    for commit in sorted(log_dir.glob("*.json")):
        for line in commit.read_text().splitlines():
            a = json.loads(line)
            if "add" in a:
                live.add(a["add"]["path"])
            elif "remove" in a:
                live.discard(a["remove"]["path"])
        per_version[int(commit.stem)] = set(live)
    boundary_by_files = next(
        v for v in sorted(per_version)
        if all((table / f).exists() for f in per_version[v]))

    # --- derivation B: the log's own VACUUM commits ---
    dt = DeltaTable(str(table))
    vacuum_ends = [h for h in dt.history() if h.get("operation") == "VACUUM END"]
    # after a vacuum, the earliest surviving version is the earliest WRITE
    # whose files were still referenced at vacuum time; with full
    # overwrites that is the last WRITE before the first vacuum commit.
    if vacuum_ends:
        last_vacuum_version = max(h["version"] for h in vacuum_ends)
        writes_before = [h["version"] for h in dt.history()
                         if h.get("operation") == "WRITE"
                         and h["version"] < last_vacuum_version]
        boundary_by_log = max(writes_before) if writes_before else None
    else:
        boundary_by_log = min(per_version)

    corroborated = (boundary_by_log == boundary_by_files)

    # --- empirical validation at the agreed boundary ---
    def readable(v: int) -> bool:
        try:
            DeltaTable(str(table), version=v).to_pyarrow_table()
            return True
        except Exception:
            return False

    validated = readable(boundary_by_files) and (
        boundary_by_files - 1 not in per_version
        or not readable(boundary_by_files - 1))

    entry = manifest.append(
        "watermark",
        chain=f"delta://{table.name}",
        boundary={"version": boundary_by_files},
        evidence_grade="derived",
        empirically_validated=validated,
        proof={
            "derivation_file_existence": boundary_by_files,
            "derivation_vacuum_commits": boundary_by_log,
            "corroborated": corroborated,
            "vacuum_end_versions": [h["version"] for h in vacuum_ends],
        })
    return entry


# ===========================================================================
# STEP 2 — Real Iceberg adapter: same contract, different metadata model
# ===========================================================================

WAREHOUSE = Path(__file__).parent / "iceberg_warehouse"


def build_iceberg_table():
    from pyiceberg.catalog.sql import SqlCatalog
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True)
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{WAREHOUSE}/catalog.db",
        warehouse=f"file://{WAREHOUSE}")
    catalog.create_namespace("sales")
    schema = pa.schema([
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("amount", pa.float64()),
    ])
    tbl = catalog.create_table("sales.orders", schema=schema)
    # 5 snapshots, each an overwrite so old files become unreferenced
    for s in range(5):
        batch = pa.table({
            "order_id": [f"O{s}{i}" for i in range(4)],
            "customer_id": [f"C{i % 2}" for i in range(4)],
            "amount": [100.0 * (s + 1) + i for i in range(4)],
        })
        tbl.overwrite(batch)
    return catalog.load_table("sales.orders")


def destroy_old_history(tbl, keep_last_n: int = 2) -> int:
    """Simulate the real-world failure mode: old data files deleted
    out-of-band (aggressive remove_orphan_files, lifecycle policy on the
    bucket, or a well-meaning cleanup script). Snapshot metadata still
    lists everything; the files are gone."""
    snapshots = sorted(tbl.metadata.snapshots, key=lambda s: s.sequence_number)
    doomed = snapshots[:-keep_last_n]
    removed = 0
    keep_files: set[str] = set()
    for snap in snapshots[-keep_last_n:]:
        for task in tbl.scan(snapshot_id=snap.snapshot_id).plan_files():
            keep_files.add(task.file.file_path)
    for snap in doomed:
        try:
            for task in tbl.scan(snapshot_id=snap.snapshot_id).plan_files():
                p = Path(task.file.file_path.replace("file://", ""))
                if task.file.file_path not in keep_files and p.exists():
                    p.unlink()
                    removed += 1
        except Exception:
            pass  # already unreadable
    return removed


def iceberg_watermark_to_manifest(tbl, manifest: Manifest) -> dict:
    """Same contract as the Delta adapter: derive boundary from snapshot
    lineage + file existence, then validate with real reads."""
    snapshots = sorted(tbl.metadata.snapshots, key=lambda s: s.sequence_number)

    def snapshot_readable(snap) -> tuple[bool, str]:
        try:
            n = tbl.scan(snapshot_id=snap.snapshot_id).to_arrow().num_rows
            return True, f"read {n} rows"
        except Exception as e:
            return False, type(e).__name__

    # Phase A+B combined: probe every snapshot with a REAL read, then
    # derive the boundary under SUFFIX semantics — the earliest snapshot
    # from which ALL later snapshots are readable. (Iceberg falsified
    # the naive single-point model: delete/append pairs create empty
    # intermediate snapshots that stay trivially readable between
    # destroyed ones. Readability is not monotone in sequence number,
    # so the honest claim is a suffix boundary PLUS the readable
    # islands below it, recorded as proof.)
    probes = []
    for snap in snapshots:
        ok, note = snapshot_readable(snap)
        probes.append((snap, ok, note))

    candidate = None
    for i, (snap, ok, _) in enumerate(probes):
        if all(ok2 for _, ok2, _ in probes[i:]):
            candidate = snap
            break

    results = {str(s.snapshot_id): f"{'READABLE' if ok else 'UNREADABLE'} ({n})"
               for s, ok, n in probes}
    islands = [str(s.snapshot_id) for s, ok, _ in probes
               if ok and s.sequence_number < candidate.sequence_number]
    validated = candidate is not None and all(
        ok for s, ok, _ in probes
        if s.sequence_number >= candidate.sequence_number)

    entry = manifest.append(
        "watermark",
        chain="iceberg://sales.orders",
        boundary={"snapshot_id": str(candidate.snapshot_id),
                  "sequence_number": candidate.sequence_number,
                  "timestamp_ms": candidate.timestamp_ms},
        evidence_grade="derived",
        empirically_validated=validated,
        proof={"snapshots_listed": len(snapshots),
               "snapshots_readable": sum(
                   1 for r in results.values() if r.startswith("READABLE")),
               "readable_islands_below_boundary": islands,
               "per_snapshot": results},
        note=("Iceberg's immutable snapshot IDs make better audit anchors "
              "than timestamps — the boundary cites an ID, not a clock."))
    return entry


# ===========================================================================

def main() -> None:
    manifest = Manifest(Path(__file__).parent / "ows_manifest.jsonl")

    print("=" * 74)
    print("  STEP 1 — Delta watermark -> hash-chained manifest (cross-checked)")
    print("=" * 74)
    delta_table = Path(__file__).parent / "lakehouse" / "sales_orders"
    entry = delta_watermark_to_manifest(delta_table, manifest)
    print(f"  boundary: version {entry['boundary']['version']}")
    print(f"  derivation A (file existence): "
          f"v{entry['proof']['derivation_file_existence']}")
    print(f"  derivation B (vacuum commits in log): "
          f"v{entry['proof']['derivation_vacuum_commits']}")
    print(f"  corroborated: {entry['proof']['corroborated']}   "
          f"empirically_validated: {entry['empirically_validated']}")
    print(f"  manifest entry seq={entry['seq']} hash={entry['hash']}")

    print("\n" + "=" * 74)
    print("  STEP 2 — Iceberg adapter: same contract, different metadata model")
    print("=" * 74)
    tbl = build_iceberg_table()
    n_snaps = len(tbl.metadata.snapshots)
    print(f"  Built real Iceberg table with {n_snaps} snapshots "
          f"(sqlite catalog, local warehouse).")
    removed = destroy_old_history(tbl, keep_last_n=2)
    print(f"  Destroyed history out-of-band: {removed} data file(s) deleted.")
    print(f"  Snapshot metadata still lists all {n_snaps} snapshots — "
          f"same lie, different format.")

    entry = iceberg_watermark_to_manifest(tbl, manifest)
    print(f"\n  boundary: snapshot {entry['boundary']['snapshot_id']} "
          f"(seq {entry['boundary']['sequence_number']})")
    print(f"  snapshots listed: {entry['proof']['snapshots_listed']}, "
          f"readable: {entry['proof']['snapshots_readable']}")
    for sid, r in entry["proof"]["per_snapshot"].items():
        print(f"    {sid[:12]}…: {r}")
    print(f"  empirically_validated: {entry['empirically_validated']}")
    print(f"  manifest entry seq={entry['seq']} hash={entry['hash']}")

    print("\n" + "=" * 74)
    print("  MANIFEST")
    print("=" * 74)
    print(f"  {len(manifest.entries)} entries, chain "
          f"{'INTACT' if manifest.verify() else 'BROKEN'} — two formats, "
          f"one contract, one ledger.")
    print(f"  persisted at {manifest.path}")


if __name__ == "__main__":
    main()

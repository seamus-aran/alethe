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
OWS in Practice — Worked Examples (Alethe reference scenarios)
==============================================================
Runnable simulations of the four load-bearing mechanisms of the
Observability Watermark Specification:

  Example 1: The watermark oracle — deriving a Delta-style boundary
             from log metadata, with EMPIRICAL validation (reads at
             boundary succeed; reads at boundary-1 fail).
  Example 2: The hash-chained manifest — append-only, tamper-evident,
             and a demonstration that tampering is detected.
  Example 3: The contradicted watermark — a countersigned OLTP source
             whose DBA silently shrinks retention; the heartbeat
             catches it and verdicts tighten conservatively.
  Example 4: The verdict engine — EXACT / BOUNDED / REFUSED over a
             temporal join with mismatched retention, including
             monotone aggregate bounds, a non-monotone refusal with
             its monotone complement, and an escrowed gap upgrade.

Everything here is simulation-grade: real logic, fake infrastructure.
Each simulated system stands in for the adapter surface the spec
defines (Delta _delta_log, Postgres catalog introspection, etc.).
"""

from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


# ===========================================================================
# Shared: the manifest (OWS §4)
# ===========================================================================

class Manifest:
    """Append-only, hash-chained ledger. Each entry commits to its
    predecessor's hash; verify() walks the chain."""

    def __init__(self) -> None:
        self.entries: list[dict] = []

    def append(self, kind: str, **payload) -> dict:
        prev_hash = self.entries[-1]["hash"] if self.entries else "GENESIS"
        body = {"seq": len(self.entries), "kind": kind,
                "prev_hash": prev_hash, **payload}
        body["hash"] = hashlib.sha256(
            json.dumps(body, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        self.entries.append(body)
        return body

    def verify(self) -> tuple[bool, Optional[int]]:
        prev = "GENESIS"
        for e in self.entries:
            if e["prev_hash"] != prev:
                return False, e["seq"]
            recomputed = hashlib.sha256(json.dumps(
                {k: v for k, v in e.items() if k != "hash"},
                sort_keys=True, default=str).encode()).hexdigest()[:16]
            if recomputed != e["hash"]:
                return False, e["seq"]
            prev = e["hash"]
        return True, None


MANIFEST = Manifest()


def banner(title: str) -> None:
    print("\n" + "=" * 74)
    print(f"  {title}")
    print("=" * 74)


# ===========================================================================
# EXAMPLE 1: The watermark oracle over a simulated Delta log
# ===========================================================================
# The subtlety the spec insists on (§5): a version can still be LISTED in
# the log while its data files were VACUUMED. The honest boundary is the
# earliest READABLE version, and conformance requires proving it
# empirically, not trusting metadata arithmetic.

@dataclass
class SimulatedDeltaTable:
    name: str
    # version -> (timestamp_day, data_files)
    log: dict[int, tuple[int, set[str]]] = field(default_factory=dict)
    storage: set[str] = field(default_factory=set)   # files that exist

    def commit(self, version: int, day: int, files: set[str]) -> None:
        self.log[version] = (day, files)
        self.storage |= files

    def vacuum(self, retain_after_day: int) -> list[str]:
        """Delete files not referenced by any version at/after the cutoff.
        This is what breaks time travel."""
        current_day = max(d for d, _ in self.log.values())
        live: set[str] = set()
        for v, (day, files) in self.log.items():
            if day >= retain_after_day:
                live |= files
        # files referenced ONLY by pre-cutoff versions get removed
        removed = sorted(self.storage - live)
        self.storage = live
        MANIFEST.append("vacuum", chain=f"delta://{self.name}",
                        cutoff_day=retain_after_day, files_removed=len(removed),
                        recorded_at_day=current_day)
        return removed

    def read_version(self, version: int) -> list[str]:
        """Time travel read. Fails if any required file was vacuumed —
        even though the log entry still exists."""
        if version not in self.log:
            raise LookupError(f"version {version} not in log")
        _, files = self.log[version]
        missing = files - self.storage
        if missing:
            raise FileNotFoundError(
                f"version {version} listed in log but unreadable: "
                f"{len(missing)} data file(s) vacuumed")
        return sorted(files)


def watermark_oracle(t: SimulatedDeltaTable) -> dict:
    """OWS adapter for the simulated Delta table. Two phases:
    (1) derive candidate boundary from metadata,
    (2) EMPIRICALLY validate: boundary readable, boundary-1 not."""
    # Phase 1: metadata derivation — earliest version whose files all exist
    candidate = None
    for v in sorted(t.log):
        _, files = t.log[v]
        if files <= t.storage:
            candidate = v
            break
    # Phase 2: empirical validation (the conformance requirement)
    proof = {}
    try:
        t.read_version(candidate)
        proof["read_at_boundary"] = "SUCCEEDED"
    except Exception as e:
        proof["read_at_boundary"] = f"FAILED ({e})"
    prior = candidate - 1
    if prior in t.log:
        try:
            t.read_version(prior)
            proof["read_below_boundary"] = "SUCCEEDED (VIOLATION!)"
        except FileNotFoundError as e:
            proof["read_below_boundary"] = f"FAILED as expected"
    else:
        proof["read_below_boundary"] = "no prior version in log"
    validated = (proof["read_at_boundary"] == "SUCCEEDED"
                 and "VIOLATION" not in proof["read_below_boundary"])
    wm = {"chain": f"delta://{t.name}",
          "boundary_version": candidate,
          "boundary_day": t.log[candidate][0],
          "evidence_grade": "derived",
          "empirically_validated": validated,
          "proof": proof}
    MANIFEST.append("watermark", **wm)
    return wm


def example_1() -> dict:
    banner("EXAMPLE 1 — Watermark oracle: derive + empirically validate")
    orders = SimulatedDeltaTable("sales/orders")
    # 100 days of daily commits
    for day in range(100):
        orders.commit(version=day, day=day, files={f"part-{day}.parquet"})
    print(f"  Built {len(orders.log)} versions over 100 days.")

    orders.vacuum(retain_after_day=70)
    print(f"  VACUUM ran with 30-day retention (cutoff = day 70).")
    print(f"  Log still lists {len(orders.log)} versions — "
          f"the log LIES about readability.")

    wm = watermark_oracle(orders)
    print(f"\n  Oracle-derived boundary: version {wm['boundary_version']} "
          f"(day {wm['boundary_day']}), grade={wm['evidence_grade']}")
    print(f"  Empirical proof:")
    for k, v in wm["proof"].items():
        print(f"    {k}: {v}")
    print(f"  Conformance: empirically_validated = "
          f"{wm['empirically_validated']}")
    return wm


# ===========================================================================
# EXAMPLE 2: Manifest tamper-evidence
# ===========================================================================

def example_2() -> None:
    banner("EXAMPLE 2 — Manifest integrity: tampering is detected")
    ok, _ = MANIFEST.verify()
    print(f"  Chain verification after Example 1: {'INTACT' if ok else 'BROKEN'}"
          f"  ({len(MANIFEST.entries)} entries)")

    print("\n  Simulating a malicious retroactive edit: an admin rewrites")
    print("  the vacuum entry to hide that files were removed...")
    victim = next(e for e in MANIFEST.entries if e["kind"] == "vacuum")
    victim["files_removed"] = 0        # the lie

    ok, seq = MANIFEST.verify()
    print(f"  Chain verification now: {'INTACT' if ok else 'BROKEN'}"
          f" — first bad entry at seq={seq}")
    print("  The hash chain converts 'quiet edit' into 'loud, located breach.'")
    # restore truth for the rest of the demo
    victim["files_removed"] = 70
    victim["hash"] = hashlib.sha256(json.dumps(
        {k: v for k, v in victim.items() if k != "hash"},
        sort_keys=True, default=str).encode()).hexdigest()[:16]
    # fix forward links
    for i in range(victim["seq"] + 1, len(MANIFEST.entries)):
        MANIFEST.entries[i]["prev_hash"] = MANIFEST.entries[i - 1]["hash"]
        e = MANIFEST.entries[i]
        e["hash"] = hashlib.sha256(json.dumps(
            {k: v for k, v in e.items() if k != "hash"},
            sort_keys=True, default=str).encode()).hexdigest()[:16]


# ===========================================================================
# EXAMPLE 3: The contradicted watermark (derived-countersigned lifecycle)
# ===========================================================================

@dataclass
class SimulatedPostgres:
    """Stands in for catalog introspection: the source SELF-REPORTS its
    retention posture; we never parse the WAL."""
    wal_retention_days: int
    slot_restart_lag_days: int   # how far back the replication slot reaches

    def introspect(self) -> dict:
        # boundary is limited by the MOST restrictive artifact
        limiting = ("replication_slot"
                    if self.slot_restart_lag_days < self.wal_retention_days
                    else "wal_retention")
        return {
            "wal_retention_days": self.wal_retention_days,
            "slot_restart_lag_days": self.slot_restart_lag_days,
            "boundary_days_back": min(self.wal_retention_days,
                                      self.slot_restart_lag_days),
            "limited_by": limiting,
        }


def example_3() -> dict:
    banner("EXAMPLE 3 — Contradicted watermark: silent retention change")
    pg = SimulatedPostgres(wal_retention_days=14, slot_restart_lag_days=14)

    # Setup: introspect -> suggest -> human countersigns
    report = pg.introspect()
    print(f"  Setup introspection: boundary reaches {report['boundary_days_back']} "
          f"days back (limited by: {report['limited_by']})")
    confirmed = MANIFEST.append(
        "countersignature", chain="pg://crm/customers",
        confirmed_boundary_days_back=report["boundary_days_back"],
        raw_introspection=report,
        identity="zach@company.example", via="sso")
    print(f"  Countersigned by {confirmed['identity']} — grade is now "
          f"derived-countersigned.")

    # Months later: a DBA shrinks WAL retention in a cost-cutting sprint.
    pg.wal_retention_days = 3
    print("\n  ...weeks pass. A DBA quietly sets WAL retention 14d -> 3d...")

    # Heartbeat re-introspection
    fresh = pg.introspect()
    confirmed_days = confirmed["confirmed_boundary_days_back"]
    if fresh["boundary_days_back"] < confirmed_days:
        MANIFEST.append("contradiction", chain="pg://crm/customers",
                        confirmed_days_back=confirmed_days,
                        observed_days_back=fresh["boundary_days_back"],
                        limited_by=fresh["limited_by"])
        effective = fresh["boundary_days_back"]  # conservative boundary
        print(f"  HEARTBEAT ALERT — contradicted watermark:")
        print(f"    confirmed: {confirmed_days} days back (signed)")
        print(f"    observed now: {fresh['boundary_days_back']} days back "
              f"(limited by {fresh['limited_by']})")
        print(f"  Verdicts tighten to the CONSERVATIVE boundary "
              f"({effective} days) until re-confirmation.")
        print(f"  This is the alert nothing on the market sends today:")
        print(f"  'your source no longer supports your audit obligations.'")
    return {"chain": "pg://crm/customers",
            "effective_days_back": fresh["boundary_days_back"],
            "state": "contradicted"}


# ===========================================================================
# EXAMPLE 4: The verdict engine
# ===========================================================================

class Verdict(Enum):
    EXACT = "EXACT"
    BOUNDED = "BOUNDED"
    REFUSED = "REFUSED"


@dataclass
class ChainState:
    chain: str
    boundary_day: int          # earliest honestly queryable day
    grade: str
    # optional escrowed aggregates for gap days: day -> {"revenue": x, "orders": n}
    escrow: dict[int, dict] = field(default_factory=dict)


def render(v: Verdict, detail: str, limiting: Optional[str]) -> None:
    mark = {"EXACT": "✓", "BOUNDED": "≈", "REFUSED": "⊘"}[v.value]
    print(f"  {mark} VERDICT: {v.value}")
    print(f"    {detail}")
    if limiting:
        print(f"    limiting link: {limiting}")


def example_4(orders_wm: dict) -> None:
    banner("EXAMPLE 4 — Verdict engine: EXACT / BOUNDED / REFUSED")

    # Chain states as an Alethe deployment would hold them
    orders = ChainState("delta://sales/orders",
                        boundary_day=orders_wm["boundary_day"],
                        grade="derived")
    customers = ChainState("delta://crm/customers", boundary_day=0,
                           grade="derived")

    # Observed data (inside orders' boundary): day -> (revenue, n_orders)
    observed = {d: (1000 + 10 * d, 5) for d in range(70, 100)}
    # Candidate knowledge for the vacuumed gap (row-shapes known from
    # surviving metadata; states unknowable) — 70 days, ~5 orders/day
    candidate_days = list(range(0, 70))

    print(f"  Chains: orders boundary=day {orders.boundary_day} (vacuumed "
          f"before), customers boundary=day {customers.boundary_day}\n")

    # --- Query A: monotone aggregate fully inside boundaries -> EXACT ---
    print("  QUERY A: SUM(revenue) for days 80-99  [monotone, all green]")
    total = sum(observed[d][0] for d in range(80, 100))
    render(Verdict.EXACT, f"revenue = {total:,} — every chain inside "
           f"boundary; grade=derived on all paths", None)

    # --- Query B: monotone aggregate crossing the boundary -> BOUNDED ---
    print("\n  QUERY B: SUM(revenue) for days 0-99  [monotone, crosses gap]")
    at_least = sum(v[0] for v in observed.values())
    render(Verdict.BOUNDED,
           f"revenue in [{at_least:,}, UNBOUNDED) — at_least from observed "
           f"days 70-99; days 0-69 are BEYOND (upper bound unknowable: "
           f"population destroyed)",
           f"{orders.chain} boundary=day {orders.boundary_day}")

    # --- Query C: same, but escrow exists -> tighter BOUNDED ---
    print("\n  QUERY C: same query, but pre-vacuum ESCROW snapshots exist")
    orders.escrow = {d: {"revenue": 1000 + 10 * d} for d in range(0, 70)}
    escrowed = sum(e["revenue"] for e in orders.escrow.values())
    render(Verdict.BOUNDED,
           f"revenue = {at_least + escrowed:,} at day-level granularity — "
           f"days 0-69 answered from escrowed aggregates "
           f"(grade=escrowed, weaker than derived; row detail still BEYOND)",
           f"{orders.chain} gap disposition=escrowed")

    # --- Query D: non-monotone across the gap -> REFUSED + complement ---
    print("\n  QUERY D: customers with NO orders, days 0-99  [NON-monotone]")
    render(Verdict.REFUSED,
           "absence of order-evidence in days 0-69 is not evidence of "
           "absence — no safe lower bound exists for a negation across "
           "a BEYOND interval",
           f"{orders.chain} boundary=day {orders.boundary_day}")
    print("    offered monotone complement: customers WITH orders in "
          "days 70-99 -> EXACT")

    # --- Query E: twice-temporal correction on a downstream gold table ---
    print("\n  QUERY E: gold.daily_revenue AS OF day 99  [materialization lag]")
    last_materialization_day = 97   # nightly job; last successful run day 97
    print(f"    gold last materialized: day {last_materialization_day} "
          f"(consumed upstream state as of day {last_materialization_day})")
    render(Verdict.BOUNDED,
           "requested day 99, but gold reflects upstream only through day "
           "97 — result is exact AS OF day 97, stale for days 98-99; "
           "certifying it as day-99 state would be the subtle dishonesty "
           "OWS exists to kill (twice-temporal correction, spec §6.2)",
           "materialization lag on edge orders->gold.daily_revenue")

    MANIFEST.append("verdict-demo-complete", queries=5)


# ===========================================================================

if __name__ == "__main__":
    wm = example_1()
    example_2()
    example_3()
    example_4(wm)

    banner("MANIFEST — final state")
    ok, _ = MANIFEST.verify()
    print(f"  {len(MANIFEST.entries)} entries, chain "
          f"{'INTACT' if ok else 'BROKEN'}. Entry kinds:")
    kinds: dict[str, int] = {}
    for e in MANIFEST.entries:
        kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
    for k, n in kinds.items():
        print(f"    {k}: {n}")

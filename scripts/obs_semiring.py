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
# The maintained equivalent lives in: alethe._semiring (packaged) / notebooks/00-01

"""
Observability Semiring Prototype
================================
A three-valued semiring for retention-aware temporal queries, where
"beyond the observability boundary" is a first-class algebraic value
that propagates through relational operators — giving honest refusal
for free, with no special-case code in the query engine.

The semiring K = {ABSENT, OBSERVED, BEYOND}:

  ABSENT   (0)  — the fact is definitively not in the data
  OBSERVED (1)  — the fact is present and inside retention
  BEYOND   (⊥?) — the question exceeds the retention boundary;
                  the honest answer is "unknowable", not "no"

Addition  (⊕) models UNION / alternative derivations.
Multiplication (⊗) models JOIN / conjunctive derivation.

    ⊕ | A  O  B          ⊗ | A  O  B
    --+---------         --+---------
    A | A  O  B          A | A  A  A
    O | O  O  O          O | A  O  B
    B | B  O  B          B | A  B  B

Intuition:
  - If any derivation path is OBSERVED, the row is observed (O absorbs in ⊕).
  - If the only alternative to ABSENT is BEYOND, we can't rule the row
    in or out: A ⊕ B = B.
  - Joining anything with ABSENT yields ABSENT (0 annihilates).
  - Joining OBSERVED with BEYOND yields BEYOND: a conjunction is only
    as knowable as its least knowable conjunct.

This is Kleene's strong 3-valued logic reinterpreted: the third value
means "outside the observability boundary", not "unknown value exists"
(SQL NULL). The payoff: standard relational algebra evaluated over K
computes, for every output row, whether that row is an answer, a
non-answer, or a refusal — soundly, compositionally, by algebra alone.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from itertools import product
from typing import Callable, Hashable


# ---------------------------------------------------------------------------
# 1. The semiring
# ---------------------------------------------------------------------------

class K(IntEnum):
    ABSENT = 0    # additive identity (semiring "0")
    BEYOND = 1    # the refusal value
    OBSERVED = 2  # multiplicative identity (semiring "1")

    def __repr__(self) -> str:
        return self.name


def k_add(a: K, b: K) -> K:
    """⊕ : union / alternative derivations. OBSERVED absorbs; BEYOND beats ABSENT."""
    return K(max(a, b))


def k_mul(a: K, b: K) -> K:
    """⊗ : join / conjunction. ABSENT annihilates; BEYOND taints OBSERVED."""
    return K(min(a, b))


ZERO, ONE = K.ABSENT, K.OBSERVED


def verify_semiring_laws() -> list[str]:
    """Exhaustively check all semiring axioms over the 3-element carrier."""
    failures = []
    E = list(K)
    for a, b, c in product(E, repeat=3):
        if k_add(k_add(a, b), c) != k_add(a, k_add(b, c)):
            failures.append(f"⊕ not associative at {a},{b},{c}")
        if k_mul(k_mul(a, b), c) != k_mul(a, k_mul(b, c)):
            failures.append(f"⊗ not associative at {a},{b},{c}")
        if k_mul(a, k_add(b, c)) != k_add(k_mul(a, b), k_mul(a, c)):
            failures.append(f"left distributivity fails at {a},{b},{c}")
        if k_mul(k_add(a, b), c) != k_add(k_mul(a, c), k_mul(b, c)):
            failures.append(f"right distributivity fails at {a},{b},{c}")
    for a, b in product(E, repeat=2):
        if k_add(a, b) != k_add(b, a):
            failures.append(f"⊕ not commutative at {a},{b}")
    for a in E:
        if k_add(a, ZERO) != a:
            failures.append(f"0 not additive identity at {a}")
        if k_mul(a, ONE) != a or k_mul(ONE, a) != a:
            failures.append(f"1 not multiplicative identity at {a}")
        if k_mul(a, ZERO) != ZERO or k_mul(ZERO, a) != ZERO:
            failures.append(f"0 not annihilating at {a}")
    return failures


# ---------------------------------------------------------------------------
# 2. K-relations: relations annotated with semiring values
# ---------------------------------------------------------------------------

Row = tuple  # immutable tuple of column values


@dataclass
class KRelation:
    """A K-annotated relation: mapping from rows to K values.

    Rows mapping to ABSENT are simply not stored (they're the implicit
    default), matching the standard K-relation formalism where the
    annotation function has finite support.
    """
    columns: tuple[str, ...]
    data: dict[Row, K] = field(default_factory=dict)

    def add(self, row: Row, ann: K) -> None:
        if ann == K.ABSENT:
            return
        existing = self.data.get(row, K.ABSENT)
        self.data[row] = k_add(existing, ann)

    # --- relational operators, all defined purely by semiring laws ---

    def select(self, pred: Callable[[dict], bool]) -> "KRelation":
        out = KRelation(self.columns)
        for row, ann in self.data.items():
            if pred(dict(zip(self.columns, row))):
                out.add(row, ann)
        return out

    def project(self, cols: tuple[str, ...]) -> "KRelation":
        """Projection merges duplicate rows with ⊕ — where refusal
        semantics really earns its keep."""
        idx = [self.columns.index(c) for c in cols]
        out = KRelation(cols)
        for row, ann in self.data.items():
            out.add(tuple(row[i] for i in idx), ann)
        return out

    def join(self, other: "KRelation", on: list[str]) -> "KRelation":
        """Natural join: matching rows multiply annotations with ⊗."""
        out_cols = self.columns + tuple(c for c in other.columns if c not in on)
        out = KRelation(out_cols)
        my_idx = [self.columns.index(c) for c in on]
        their_idx = [other.columns.index(c) for c in on]
        their_rest = [i for i, c in enumerate(other.columns) if c not in on]
        # hash join on the key
        buckets: dict[tuple, list[tuple[Row, K]]] = {}
        for row, ann in other.data.items():
            buckets.setdefault(tuple(row[i] for i in their_idx), []).append((row, ann))
        for row, ann in self.data.items():
            key = tuple(row[i] for i in my_idx)
            for orow, oann in buckets.get(key, []):
                combined = k_mul(ann, oann)
                if combined != K.ABSENT:
                    out.add(row + tuple(orow[i] for i in their_rest), combined)
        return out

    def union(self, other: "KRelation") -> "KRelation":
        out = KRelation(self.columns)
        for row, ann in self.data.items():
            out.add(row, ann)
        for row, ann in other.data.items():
            out.add(row, ann)
        return out

    def show(self, title: str = "") -> None:
        if title:
            print(f"\n  {title}")
        header = " | ".join(f"{c:<14}" for c in self.columns) + " | annotation"
        print("  " + header)
        print("  " + "-" * len(header))
        for row, ann in sorted(self.data.items(), key=lambda x: str(x[0])):
            cells = " | ".join(f"{str(v):<14}" for v in row)
            print(f"  {cells} | {ann!r}")
        if not self.data:
            print("  (empty)")


# ---------------------------------------------------------------------------
# 3. Temporal layer: retention-aware point-in-time snapshots
# ---------------------------------------------------------------------------

@dataclass
class TemporalTable:
    """A bitemporal-ish table: each version of a row carries a system-time
    interval [valid_from, valid_to). The table also carries a retention
    watermark: history before it has been vacuumed/expired.

    `as_of(t)` produces a K-relation:
      - t >= watermark: rows valid at t are OBSERVED. Clean snapshot.
      - t <  watermark: history is gone. Every *entity* this table has
        ever known about is annotated BEYOND — we cannot say what its
        state was, nor even whether it existed at t. The algebra takes
        it from there.
    """
    name: str
    columns: tuple[str, ...]          # logical columns (no temporal cols)
    key_columns: tuple[str, ...]      # entity key
    retention_watermark: int          # earliest system time still observable
    versions: list[tuple[Row, int, int]] = field(default_factory=list)
    INF = 10**9

    def insert_version(self, row: Row, valid_from: int, valid_to: int = INF):
        self.versions.append((row, valid_from, valid_to))

    def as_of(self, t: int) -> KRelation:
        rel = KRelation(self.columns)
        if t >= self.retention_watermark:
            for row, vf, vt in self.versions:
                if vf <= t < vt:
                    rel.add(row, K.OBSERVED)
            return rel
        # Query predates retention. We cannot reconstruct the state at t,
        # but we know which row-shapes this table has ever contained. Emit
        # each distinct known row as a CANDIDATE annotated BEYOND: the
        # annotation — not value masking — is what marks it untrustworthy.
        # (Masking the join key would make BEYOND rows silently vanish
        # from joins, producing exactly the false confidence we're trying
        # to eliminate. Candidates keep the algebra flowing; ⊗ taints
        # everything they touch.)
        seen: set[Row] = set()
        for row, _, _ in self.versions:
            if row not in seen:
                seen.add(row)
                rel.add(row, K.BEYOND)
        return rel


@dataclass
class QueryResult:
    answers: KRelation           # rows annotated OBSERVED
    refusals: KRelation          # rows annotated BEYOND
    refused: bool                # did the query exceed any observability boundary?

    def report(self, title: str) -> None:
        print("\n" + "=" * 72)
        print(f"  QUERY: {title}")
        print("=" * 72)
        self.answers.show("ANSWERS (observed, inside retention):")
        if self.refused:
            self.refusals.show("REFUSALS (beyond observability boundary):")
            print("\n  ⚠ HONEST REFUSAL: parts of this answer are unknowable —")
            print("    the query's time bound precedes a retention watermark.")
            print("    These rows are neither confirmed nor denied.")
        else:
            print("\n  ✓ Complete answer: fully inside all observability boundaries.")


def split_result(rel: KRelation) -> QueryResult:
    """Partition a K-relation into answers vs refusals. This is the ONLY
    place refusal is 'handled' — everything upstream is plain algebra."""
    answers = KRelation(rel.columns)
    refusals = KRelation(rel.columns)
    for row, ann in rel.data.items():
        (answers if ann == K.OBSERVED else refusals).add(row, ann)
    return QueryResult(answers, refusals, refused=bool(refusals.data))


# ---------------------------------------------------------------------------
# 4. Demo: a temporal join across mismatched retention boundaries
# ---------------------------------------------------------------------------

def demo() -> None:
    print("=" * 72)
    print("  OBSERVABILITY SEMIRING — retention-aware temporal join prototype")
    print("=" * 72)

    fails = verify_semiring_laws()
    n_checked = 27 * 4 + 9 + 3 * 3  # triples×4 laws + comm pairs + identities
    print(f"\n  Semiring law verification: {'ALL LAWS HOLD' if not fails else fails}")
    print(f"  (exhaustive over the 3-element carrier, {n_checked} identities checked)")

    # -- customers: long retention (history back to t=0) ------------------
    customers = TemporalTable(
        name="customers",
        columns=("customer_id", "segment"),
        key_columns=("customer_id",),
        retention_watermark=0,
    )
    customers.insert_version(("C1", "smb"), 0, 50)
    customers.insert_version(("C1", "enterprise"), 50)      # upgraded at t=50
    customers.insert_version(("C2", "smb"), 10)
    customers.insert_version(("C3", "enterprise"), 30)

    # -- orders: short retention! history before t=40 was vacuumed --------
    orders = TemporalTable(
        name="orders",
        columns=("order_id", "customer_id", "amount"),
        key_columns=("order_id",),
        retention_watermark=40,
    )
    orders.insert_version(("O1", "C1", 100), 20)   # created t=20 (pre-watermark!)
    orders.insert_version(("O2", "C2", 250), 45)
    orders.insert_version(("O3", "C1", 900), 60)

    def run(t: int) -> None:
        c = customers.as_of(t)
        o = orders.as_of(t)
        joined = o.join(c, on=["customer_id"])
        result = split_result(joined)
        result.report(f"orders ⋈ customers AS OF t={t}   "
                      f"(orders watermark=40, customers watermark=0)")

    # Query 1: safely inside both retention boundaries
    run(t=60)

    # Query 2: inside customers' retention, BEYOND orders' retention.
    # The join taints every row through ⊗ — refusal propagates by algebra.
    run(t=30)

    # Query 3: projection + ⊕ interplay — "which segments had any order at t?"
    print("\n" + "=" * 72)
    print("  QUERY: distinct segments with orders AS OF t=30 (projection/⊕ demo)")
    print("=" * 72)
    c, o = customers.as_of(30), orders.as_of(30)
    seg = o.join(c, on=["customer_id"]).project(("segment",))
    split_result(seg).report("π_segment(orders ⋈ customers) AS OF t=30")
    print("\n  Note: order history at t=30 is vacuumed, so these segments are")
    print("  candidates, not facts. A naive engine returns an empty (falsely")
    print("  confident) answer; the semiring returns a refusal — 'unknowable',")
    print("  not 'no'. The BEYOND taint flowed from orders through ⊗ into the")
    print("  join, then survived projection through ⊕. Zero special-case code.")


if __name__ == "__main__":
    demo()

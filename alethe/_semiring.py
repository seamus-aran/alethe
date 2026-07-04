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

"""Three-valued observability semiring K = {ABSENT, BEYOND, OBSERVED}.

ABSENT   (0) — fact is definitively absent.
BEYOND   (1) — query exceeds the retention boundary; honest refusal.
OBSERVED (2) — fact is present and inside retention.

⊕ = max (union/alternatives); OBSERVED absorbs.
⊗ = min (join/conjunction); ABSENT annihilates, BEYOND taints OBSERVED.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from itertools import product
from typing import Callable


class K(IntEnum):
    ABSENT = 0
    BEYOND = 1
    OBSERVED = 2

    def __repr__(self) -> str:
        return self.name


def k_add(a: K, b: K) -> K:
    """⊕ : union / alternative derivations."""
    return K(max(a, b))


def k_mul(a: K, b: K) -> K:
    """⊗ : join / conjunction."""
    return K(min(a, b))


ZERO, ONE = K.ABSENT, K.OBSERVED


def verify_semiring_laws() -> list[str]:
    """Exhaustively verify all semiring axioms over the 3-element carrier."""
    failures: list[str] = []
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


Row = tuple


@dataclass
class KRelation:
    """A K-annotated relation mapping rows to K values."""
    columns: tuple[str, ...]
    data: dict[Row, K] = field(default_factory=dict)

    def add(self, row: Row, ann: K) -> None:
        if ann == K.ABSENT:
            return
        self.data[row] = k_add(self.data.get(row, K.ABSENT), ann)

    def select(self, pred: Callable[[dict], bool]) -> "KRelation":
        out = KRelation(self.columns)
        for row, ann in self.data.items():
            if pred(dict(zip(self.columns, row))):
                out.add(row, ann)
        return out

    def project(self, cols: tuple[str, ...]) -> "KRelation":
        idx = [self.columns.index(c) for c in cols]
        out = KRelation(cols)
        for row, ann in self.data.items():
            out.add(tuple(row[i] for i in idx), ann)
        return out

    def join(self, other: "KRelation", on: list[str]) -> "KRelation":
        out_cols = self.columns + tuple(c for c in other.columns if c not in on)
        out = KRelation(out_cols)
        my_idx = [self.columns.index(c) for c in on]
        their_idx = [other.columns.index(c) for c in on]
        their_rest = [i for i, c in enumerate(other.columns) if c not in on]
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


@dataclass
class TemporalTable:
    """Bitemporal table with a retention watermark.

    `as_of(t)` returns OBSERVED rows for t >= watermark, or BEYOND
    candidates for every known entity when t < watermark — so BEYOND
    taint propagates through joins by algebra alone.
    """
    name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    retention_watermark: int
    versions: list[tuple[Row, int, int]] = field(default_factory=list)
    INF: int = 10 ** 9

    def insert_version(self, row: Row, valid_from: int, valid_to: int = 10 ** 9) -> None:
        self.versions.append((row, valid_from, valid_to))

    def as_of(self, t: int) -> KRelation:
        rel = KRelation(self.columns)
        if t >= self.retention_watermark:
            for row, vf, vt in self.versions:
                if vf <= t < vt:
                    rel.add(row, K.OBSERVED)
            return rel
        seen: set[Row] = set()
        for row, _, _ in self.versions:
            if row not in seen:
                seen.add(row)
                rel.add(row, K.BEYOND)
        return rel


@dataclass
class QueryResult:
    answers: KRelation
    refusals: KRelation
    refused: bool


def split_result(rel: KRelation) -> QueryResult:
    """Partition a K-relation into answers (OBSERVED) vs refusals (BEYOND)."""
    answers = KRelation(rel.columns)
    refusals = KRelation(rel.columns)
    for row, ann in rel.data.items():
        (answers if ann == K.OBSERVED else refusals).add(row, ann)
    return QueryResult(answers, refusals, refused=bool(refusals.data))

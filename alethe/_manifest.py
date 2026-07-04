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

"""Append-only, hash-chained JSONL manifest for OWS watermark entries."""

from __future__ import annotations
import hashlib
import json
from pathlib import Path


class Manifest:
    """Append-only, hash-chained JSONL ledger.

    Each entry commits to its predecessor's hash. Corrections are new
    entries, never edits — the chain detects any tampering.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.entries: list[dict] = []
        if self.path.exists():
            self.entries = [
                json.loads(line)
                for line in self.path.read_text().splitlines()
                if line.strip()
            ]

    def append(self, kind: str, **payload) -> dict:
        prev_hash = self.entries[-1]["hash"] if self.entries else "GENESIS"
        body = {"seq": len(self.entries), "kind": kind,
                "prev_hash": prev_hash, **payload}
        body["hash"] = hashlib.sha256(
            json.dumps(body, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        self.entries.append(body)
        self.path.write_text(
            "\n".join(json.dumps(e, default=str) for e in self.entries))
        return body

    def verify(self) -> bool:
        """Return True iff every entry's hash matches its content and chain."""
        prev = "GENESIS"
        for e in self.entries:
            if e["prev_hash"] != prev:
                return False
            body = {k: v for k, v in e.items() if k != "hash"}
            expected = hashlib.sha256(
                json.dumps(body, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]
            if expected != e["hash"]:
                return False
            prev = e["hash"]
        return True

    def __len__(self) -> int:
        return len(self.entries)

    def __repr__(self) -> str:
        intact = "INTACT" if self.verify() else "BROKEN"
        return f"Manifest({self.path.name!r}, {len(self.entries)} entries, {intact})"

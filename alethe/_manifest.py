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

"""Append-only, hash-chained JSONL manifest for OWS watermark entries.

Supports local filesystem paths and S3 URIs (``s3://bucket/key``).
GCS and ADLS support follows the same pattern — contributions welcome.

S3 notes
--------
- Requires ``pip install alethe[s3]`` (adds boto3).
- Read-modify-write is NOT atomic.  Concurrent writes from multiple
  processes will silently lose entries.  For production, serialize
  writes through a single writer process or use a distributed lock.
"""

from __future__ import annotations
import hashlib
import json
from pathlib import Path


class Manifest:
    """Append-only, hash-chained JSONL ledger.

    Each entry commits to its predecessor's hash. Corrections are new
    entries, never edits — the chain detects any tampering.

    Parameters
    ----------
    path:
        Local filesystem path (str or ``pathlib.Path``) or an S3 URI
        of the form ``s3://bucket/path/to/manifest.jsonl``.
    """

    def __init__(self, path: str | Path):
        self._raw_path = str(path)
        self._is_s3 = self._raw_path.startswith("s3://")
        self.entries: list[dict] = []

        if self._is_s3:
            self._s3_bucket, self._s3_key = _parse_s3_uri(self._raw_path)
            text = self._s3_read()
        else:
            self.path = Path(path)
            text = self.path.read_text() if self.path.exists() else ""

        if text.strip():
            self.entries = [
                json.loads(line)
                for line in text.splitlines()
                if line.strip()
            ]

    # ------------------------------------------------------------------

    def append(self, kind: str, **payload) -> dict:
        prev_hash = self.entries[-1]["hash"] if self.entries else "GENESIS"
        body = {"seq": len(self.entries), "kind": kind,
                "prev_hash": prev_hash, **payload}
        body["hash"] = hashlib.sha256(
            json.dumps(body, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        self.entries.append(body)
        text = "\n".join(json.dumps(e, default=str) for e in self.entries)
        if self._is_s3:
            self._s3_write(text)
        else:
            self.path.write_text(text)
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

    # ------------------------------------------------------------------
    # S3 helpers

    def _s3_read(self) -> str:
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "S3 manifest support requires boto3: "
                "pip install alethe[s3]") from e
        s3 = boto3.client("s3")
        try:
            resp = s3.get_object(Bucket=self._s3_bucket, Key=self._s3_key)
            return resp["Body"].read().decode()
        except s3.exceptions.NoSuchKey:
            return ""
        except Exception as exc:
            # Handle both botocore ClientError (NoSuchKey) and other errors.
            if "NoSuchKey" in str(exc):
                return ""
            raise

    def _s3_write(self, text: str) -> None:
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "S3 manifest support requires boto3: "
                "pip install alethe[s3]") from e
        boto3.client("s3").put_object(
            Bucket=self._s3_bucket,
            Key=self._s3_key,
            Body=text.encode(),
            ContentType="application/x-ndjson",
        )

    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.entries)

    def __repr__(self) -> str:
        intact = "INTACT" if self.verify() else "BROKEN"
        label = self._raw_path if self._is_s3 else Path(self._raw_path).name
        return f"Manifest({label!r}, {len(self.entries)} entries, {intact})"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/path/to/key`` → ``("bucket", "path/to/key")``."""
    without_scheme = uri[5:]  # strip "s3://"
    bucket, _, key = without_scheme.partition("/")
    return bucket, key

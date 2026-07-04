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

"""alethe CLI — CI gate for point-in-time achievability.

Usage:

    alethe check \\
        --dbt-manifest target/manifest.json \\
        --model revenue_summary \\
        --as-of 2024-03-01 \\
        --watermarks watermarks.jsonl \\
        [--run-results target/run_results.json] \\
        [--allow-bounded]

Exit codes:
    0  CERTAIN (or BOUNDED with --allow-bounded)
    1  BOUNDED without --allow-bounded
    2  UNACHIEVABLE
    3  usage / resolution error
"""

from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone


def _parse_as_of(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def check(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        prog="alethe check",
        description="Fail the build when a model cannot honestly answer "
                    "a point-in-time query.")
    p.add_argument("--dbt-manifest", required=True,
                   help="Path to dbt target/manifest.json")
    p.add_argument("--model", required=True,
                   help="dbt model name or unique_id")
    p.add_argument("--as-of", required=True,
                   help="Point in time (ISO-8601; naive input is UTC)")
    p.add_argument("--watermarks", required=True,
                   help="Recorded alethe manifest (JSONL, local or s3://)")
    p.add_argument("--run-results", default=None,
                   help="dbt run_results.json for the twice-temporal check")
    p.add_argument("--allow-bounded", action="store_true",
                   help="Exit 0 for BOUNDED (monotone aggregates only)")
    args = p.parse_args(argv)

    from . import load_watermarks
    from ._models import PitStatus
    from .integrations.dbt import DbtLineage

    try:
        as_of = _parse_as_of(args.as_of)
        chains = load_watermarks(args.watermarks)
        lineage = DbtLineage(args.dbt_manifest)
        wms = lineage.resolve_watermarks(args.model, chains)
        report = lineage.pit_report(
            args.model, watermarks=wms,
            run_results_path=args.run_results)
    except (ValueError, KeyError, FileNotFoundError) as e:
        print(f"alethe: error: {e}", file=sys.stderr)
        return 3

    print(report)
    print()

    if report.materialization_conformant is False:
        print("⚠  twice-temporal NON-CONFORMANT: the model was materialized "
              "before the upstream retention boundary (spec §6).")

    zone = report.query(as_of)
    if zone.status == PitStatus.CERTAIN:
        print(f"CERTAIN — {args.model} AS OF {as_of.isoformat()} is fully "
              "answerable.")
        return 0
    if zone.status == PitStatus.BOUNDED:
        print(f"BOUNDED — retention has destroyed part of "
              f"{zone.limiting_chains} at {as_of.isoformat()}. Monotone "
              "aggregates are lower bounds; non-monotone queries are "
              "unsound.")
        return 0 if args.allow_bounded else 1
    print(f"UNACHIEVABLE — {zone.limiting_chains} did not exist at "
          f"{as_of.isoformat()}. The population itself is unknowable.")
    return 2


def main() -> None:
    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)
    if argv[0] == "check":
        sys.exit(check(argv[1:]))
    print(f"alethe: unknown command {argv[0]!r} (try: alethe check --help)",
          file=sys.stderr)
    sys.exit(3)


if __name__ == "__main__":
    main()

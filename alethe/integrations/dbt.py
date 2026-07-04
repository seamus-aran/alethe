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

"""dbt manifest integration for alethe.

Reads a dbt ``target/manifest.json`` and walks the model DAG to find all
upstream sources for any model, then composes their watermarks into a
PIT achievability report using weakest-link semantics.

No dbt installation required — the manifest is plain JSON.  Supports
manifest schema versions v1–v12 (dbt Core 0.19–1.11+).

Leaf node types
---------------
By default, BFS stops and records a node as needing a watermark when its
``resource_type`` is ``"source"`` or ``"snapshot"``.  Seeds
(``resource_type == "seed"``) are skipped — they are static CSV files.
Pass ``leaf_types`` to override.

Example
-------
    from alethe.integrations.dbt import DbtLineage
    import alethe

    lineage = DbtLineage("target/manifest.json")

    # Option 1: supply pre-computed watermarks keyed by source unique_id
    watermarks = {
        "source.my_project.raw.orders":    alethe.watermark("/data/orders"),
        "source.my_project.raw.customers": alethe.watermark("/data/customers"),
    }
    report = lineage.pit_report("revenue_summary", watermarks=watermarks)
    print(report)

    # Option 2: resolver function receives the source node dict
    def resolve(node: dict):
        return alethe.watermark(f"/data/{node['schema']}/{node['name']}")

    report = lineage.pit_report("revenue_summary", resolver=resolve)
"""

from __future__ import annotations
import json
from collections import deque
from pathlib import Path
from typing import Callable

from .._lineage import pit_report as _pit_report
from .._models import PitReport, Watermark


_LEAF_TYPES: frozenset[str] = frozenset({"source", "snapshot"})


class DbtLineage:
    """Wraps a dbt manifest and exposes watermark-aware PIT reporting.

    Parameters
    ----------
    manifest_path:
        Path to ``target/manifest.json`` produced by ``dbt compile`` or
        ``dbt run``.  Supports manifest schema versions v1–v12
        (dbt Core 0.19–1.11+).
    leaf_types:
        ``resource_type`` values that terminate BFS and require a
        watermark.  Default: ``{"source", "snapshot"}``.  Seeds are
        deliberately excluded — they are static CSV files.
    """

    def __init__(self, manifest_path: str | Path,
                 leaf_types: frozenset[str] | set[str] = _LEAF_TYPES) -> None:
        raw = json.loads(Path(manifest_path).read_text())
        self._manifest_path = Path(manifest_path)
        self._leaf_types = frozenset(leaf_types)
        self._schema_version: str = (
            raw.get("metadata", {}).get("dbt_schema_version", "unknown"))
        # Merge nodes and sources into one lookup so DAG traversal is uniform.
        self._nodes: dict[str, dict] = {**raw.get("nodes", {}),
                                         **raw.get("sources", {})}

    # ------------------------------------------------------------------
    # Discovery helpers

    @property
    def schema_version(self) -> str:
        """The ``metadata.dbt_schema_version`` string from the manifest."""
        return self._schema_version

    def models(self) -> list[str]:
        """Return short names of all model nodes."""
        return [n["name"] for n in self._nodes.values()
                if n.get("resource_type") == "model"]

    def leaf_nodes(self) -> list[str]:
        """Return unique_ids of all leaf nodes (sources + snapshots by default)."""
        return [uid for uid, n in self._nodes.items()
                if n.get("resource_type") in self._leaf_types]

    def upstream_leaves(self, model_name: str) -> list[dict]:
        """BFS from ``model_name`` to all reachable leaf nodes.

        BFS terminates at nodes whose ``resource_type`` is in
        ``leaf_types`` (default: sources and snapshots).  Unknown node
        IDs (macros, cross-project refs) are silently skipped — they
        have no data files to watermark.

        Parameters
        ----------
        model_name:
            Short name (e.g. ``"revenue_summary"``) or full unique_id
            (e.g. ``"model.my_project.revenue_summary"``).

        Returns
        -------
        list of leaf node dicts.  Source nodes contain at minimum
        ``unique_id``, ``source_name``, ``name``, ``schema``.
        Snapshot nodes contain ``unique_id``, ``name``, ``config``.
        """
        root = self._resolve(model_name)
        visited: set[str] = set()
        queue: deque[str] = deque([root])
        found: list[dict] = []

        while queue:
            uid = queue.popleft()
            if uid in visited:
                continue
            visited.add(uid)
            node = self._nodes.get(uid)
            if node is None:
                # Silently skip macros, cross-project refs, missing nodes.
                continue
            if node.get("resource_type") in self._leaf_types:
                found.append(node)
            else:
                for dep in node.get("depends_on", {}).get("nodes", []):
                    if dep not in visited:
                        queue.append(dep)

        return found

    def upstream_sources(self, model_name: str) -> list[dict]:
        """Alias for ``upstream_leaves`` for backwards compatibility."""
        return self.upstream_leaves(model_name)

    # ------------------------------------------------------------------
    # PIT report

    def pit_report(
        self,
        model_name: str,
        *,
        watermarks: dict[str, Watermark] | None = None,
        resolver: Callable[[dict], Watermark] | None = None,
    ) -> PitReport:
        """Build a PIT achievability report for a downstream dbt model.

        Exactly one of ``watermarks`` or ``resolver`` must be supplied.

        Parameters
        ----------
        model_name:
            Short model name or full unique_id.
        watermarks:
            Dict mapping source ``unique_id`` → ``Watermark``.  Use this
            when you have pre-computed watermarks.
        resolver:
            Callable that receives a source node dict and returns a
            ``Watermark``.  Use this for on-the-fly computation.
        """
        if (watermarks is None) == (resolver is None):
            raise ValueError("Supply exactly one of `watermarks` or `resolver`.")

        sources = self.upstream_leaves(model_name)
        if not sources:
            raise ValueError(
                f"No upstream sources found for model '{model_name}'. "
                "Check that the model exists and its DAG is fully compiled.")

        wms: list[Watermark] = []
        missing: list[str] = []
        for src in sources:
            uid = src["unique_id"]
            if watermarks is not None:
                if uid not in watermarks:
                    missing.append(uid)
                else:
                    wms.append(watermarks[uid])
            else:
                wms.append(resolver(src))  # type: ignore[misc]

        if missing:
            raise ValueError(
                f"Missing watermarks for sources: {missing}. "
                "Add them to the `watermarks` dict or use a `resolver`.")

        return _pit_report(model_name, wms)

    # ------------------------------------------------------------------

    def _resolve(self, name: str) -> str:
        """Accept a short name or full unique_id; return unique_id."""
        if name in self._nodes:
            return name
        matches = [uid for uid, n in self._nodes.items()
                   if n.get("name") == name and n.get("resource_type") == "model"]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous model name '{name}' — found {matches}. "
                "Pass the full unique_id instead.")
        raise KeyError(f"Model '{name}' not found in manifest.")

    def __repr__(self) -> str:
        n_models = len(self.models())
        n_leaves = len(self.leaf_nodes())
        return (f"DbtLineage({self._manifest_path.name!r}, "
                f"{n_models} models, {n_leaves} leaf nodes, "
                f"schema={self._schema_version!r})")

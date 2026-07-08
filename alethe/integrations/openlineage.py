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

"""OpenLineage integration for alethe.

Converts alethe ``Watermark`` objects to and from OpenLineage-compatible
dataset facets so watermark metadata can flow through Marquez, Astronomer
Observe, or any OpenLineage-compatible catalog.

No ``openlineage-python`` package required — all output is plain dicts
that conform to the OpenLineage spec.

OL custom facet schema URI:
  https://github.com/seamus-aran/alethe/blob/main/alethe/integrations/openlineage.py

Example — Airflow operator
--------------------------
Emit a run event from an Airflow task that reads watermarked tables:

    from airflow.lineage import apply_lineage
    from alethe.integrations.openlineage import to_run_event
    import alethe, requests

    wm_orders    = alethe.watermark("/data/orders")
    wm_customers = alethe.watermark("/data/customers")

    event = to_run_event(
        job_name="revenue_summary",
        job_namespace="my_airflow_dag",
        inputs=[wm_orders, wm_customers],
        output_namespace="warehouse",
        output_name="reporting.revenue_summary",
    )
    requests.post("http://marquez:5000/api/v1/lineage", json=event)

Example — parse facet from incoming OL event
--------------------------------------------
    from alethe.integrations.openlineage import from_facet

    # dataset facet dict received from Marquez / Astronomer
    wm = from_facet("delta://orders", facet_dict)
    report = alethe.pit_report("downstream_model", [wm])
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

from .._models import EvidenceGrade, Watermark, parse_dt

_PRODUCER = "https://github.com/seamus-aran/alethe"
_FACET_KEY = "observabilityWatermark"
_SCHEMA_URL = (
    "https://raw.githubusercontent.com/seamus-aran/alethe/main/"
    "alethe/integrations/openlineage.py"
)


def to_facet(wm: Watermark) -> dict[str, Any]:
    """Convert a ``Watermark`` to an OpenLineage custom dataset facet.

    The facet is keyed ``"observabilityWatermark"`` and carries the full
    watermark payload.  Attach it to the ``facets`` dict of any OL
    ``Dataset`` object or inline in a run-event payload.

    Returns
    -------
    dict suitable for inclusion in ``dataset.facets``.
    """
    return {
        _FACET_KEY: {
            "_producer": _PRODUCER,
            "_schemaURL": _SCHEMA_URL,
            "chain": wm.chain,
            "boundary": wm.boundary,
            "boundaryTime": wm.boundary_dt.isoformat(),
            "earliestTime": wm.earliest_dt.isoformat(),
            "evidenceGrade": wm.evidence_grade.value,
            "empiricallyValidated": wm.empirically_validated,
            "claimRecordedAt": wm.claim_recorded_at.isoformat(),
            "readableIslands": wm.readable_islands,
            "proof": wm.proof,
        }
    }


def to_run_event(
    *,
    job_name: str,
    job_namespace: str,
    inputs: list[Watermark],
    output_name: str,
    output_namespace: str = "alethe",
    run_id: str | None = None,
    event_time: datetime | None = None,
    event_type: str = "COMPLETE",
) -> dict[str, Any]:
    """Build a complete OpenLineage ``RunEvent`` dict.

    Each input ``Watermark`` becomes an OL ``InputDataset`` with an
    ``observabilityWatermark`` facet.  The output dataset is recorded
    without a watermark facet (it has not yet been watermarked).

    Parameters
    ----------
    job_name:
        Name of the job / dbt model / Airflow task.
    job_namespace:
        OL namespace, e.g. the dbt project name or Airflow DAG id.
    inputs:
        Upstream watermarks.  Each watermark's ``chain`` field is parsed
        to derive the OL dataset namespace and name (``delta://orders``
        → namespace ``"delta"``, name ``"orders"``).
    output_name:
        Logical name of the output dataset.
    output_namespace:
        OL namespace for the output.
    run_id:
        UUID for the run.  A random UUID is generated if omitted.
    event_time:
        ISO-8601 event timestamp.  Defaults to now (UTC).
    event_type:
        OL event type: ``"START"``, ``"COMPLETE"``, ``"FAIL"``.
    """
    import uuid as _uuid
    rid = run_id or str(_uuid.uuid4())
    ts = (event_time or datetime.now(tz=timezone.utc)).isoformat()

    input_datasets = []
    for wm in inputs:
        ns, name = _parse_chain(wm.chain)
        input_datasets.append({
            "namespace": ns,
            "name": name,
            "facets": to_facet(wm),
        })

    return {
        "eventType": event_type,
        "eventTime": ts,
        "run": {"runId": rid},
        "job": {"namespace": job_namespace, "name": job_name},
        "inputs": input_datasets,
        "outputs": [{"namespace": output_namespace, "name": output_name}],
        "producer": _PRODUCER,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }


def from_facet(chain: str, facets: dict[str, Any]) -> Watermark:
    """Reconstruct a ``Watermark`` from an OpenLineage dataset facets dict.

    Useful when consuming OL events from Marquez or Astronomer to feed
    back into ``alethe.pit_report()``.

    Parameters
    ----------
    chain:
        The ``chain`` identifier to associate with the watermark, e.g.
        ``"delta://orders"``.
    facets:
        The ``facets`` dict from an OL ``Dataset`` — the watermark is
        read from ``facets["observabilityWatermark"]``.
    """
    f = facets.get(_FACET_KEY)
    if f is None:
        raise ValueError(
            f"No '{_FACET_KEY}' facet found. "
            f"Available facets: {list(facets)}")

    return Watermark(
        chain=f.get("chain", chain),
        boundary=f["boundary"],
        boundary_dt=parse_dt(f["boundaryTime"]),
        earliest_dt=parse_dt(f["earliestTime"]),
        evidence_grade=EvidenceGrade(f["evidenceGrade"]),
        empirically_validated=f.get("empiricallyValidated", False),
        proof=f.get("proof", {}),
        claim_recorded_at=parse_dt(f["claimRecordedAt"]),
        readable_islands=f.get("readableIslands", []),
    )


# ------------------------------------------------------------------
# Internal helpers

def _parse_chain(chain: str) -> tuple[str, str]:
    """Split ``"delta://orders"`` → ``("delta", "orders")``.

    For dotted names like ``"iceberg://sales.orders"``, the namespace
    becomes ``"iceberg"`` and the name becomes ``"sales.orders"``.
    """
    if "://" in chain:
        proto, rest = chain.split("://", 1)
        return proto, rest
    return "alethe", chain

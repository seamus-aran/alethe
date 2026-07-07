# Copyright 2026 Caelan Cooper
# Licensed under the Apache License, Version 2.0.

"""Gated reconstruction DAG for the alethe Airflow POC.

Three tasks so the PIT gating is visible in the Airflow task states:

  resolve_zone      → load watermarks, weakest-link PIT report over BOTH
                      chains, record the verdict in the manifest, skip
                      (AirflowSkipException) when UNACHIEVABLE.
  rebuild_partition → CERTAIN: real timestamp time travel; BOUNDED:
                      clamped read at the boundary version. Joins
                      dim_customer + dim_date, writes a parquet partition
                      stamped with pit_verdict.
  validate_partition→ reads the partition back and asserts the stamp.

The naive_reconstruct DAG is the ungated baseline: a raw AS OF read that
explodes at runtime on vacuumed dates.

Paths come from the POC_AIRFLOW_ROOT env var (set by the notebook).
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException


def _root() -> Path:
    return Path(os.environ["POC_AIRFLOW_ROOT"])


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False,
     tags=["alethe", "poc"])
def reconstruct_revenue():

    @task
    def resolve_zone(logical_date=None):
        import alethe
        root = _root()
        chains = alethe.load_watermarks(root / "watermarks.jsonl")
        wm_orders = chains["delta://fct_orders"]
        wm_cust = chains["delta://dim_customer"]

        report = alethe.pit_report(
            "reporting.revenue_daily", [wm_orders, wm_cust])
        zone = report.query(logical_date)

        # Land the verdict in the tamper-evident ledger (audit trail)
        alethe.record_report(report, root / "watermarks.jsonl",
                             as_of=logical_date)

        if zone.status.value == "UNACHIEVABLE":
            raise AirflowSkipException(
                f"{logical_date}: {zone.limiting_chains} did not exist — "
                "refusing to fabricate.")

        return {
            "status": zone.status.value,
            "limiting": zone.limiting_chains,
            "orders_boundary_version": wm_orders.boundary["version"],
            "cust_boundary_version": wm_cust.boundary["version"],
        }

    @task
    def rebuild_partition(zone: dict, logical_date=None):
        import pandas as pd
        from deltalake import DeltaTable
        root = _root()

        def pit_read(table_path: Path, boundary_version: int) -> "pd.DataFrame":
            dt = DeltaTable(str(table_path))
            if zone["status"] == "BOUNDED":
                # State at logical_date is destroyed; the boundary is the
                # best available evidence — clamped, and stamped below.
                dt.load_as_version(boundary_version)
            else:
                dt.load_as_version(logical_date)  # real time travel
            return dt.to_pyarrow_table().to_pandas()

        orders = pit_read(root / "data" / "fct_orders",
                          zone["orders_boundary_version"])
        customers = pit_read(root / "data" / "dim_customer",
                             zone["cust_boundary_version"])
        dates = pd.read_parquet(root / "data" / "dim_date.parquet")

        df = (orders.merge(customers, on="customer_id")
                    .merge(dates, on="day_key"))
        out = (df.groupby(["segment", "day_name"], as_index=False)["amount"]
                 .sum().rename(columns={"amount": "revenue"}))
        out["pit_verdict"] = zone["status"]

        ds = logical_date.strftime("%H%M%S")
        part_dir = root / "output" / f"ds={ds}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out.to_parquet(part_dir / "part.parquet", index=False)
        return {"ds": ds, "rows": len(out), "verdict": zone["status"]}

    @task
    def validate_partition(meta: dict):
        import pandas as pd
        root = _root()
        df = pd.read_parquet(root / "output" / f"ds={meta['ds']}" / "part.parquet")
        assert len(df) > 0, "empty partition"
        assert "pit_verdict" in df.columns, "missing epistemic stamp"
        assert (df.pit_verdict == meta["verdict"]).all(), "verdict mismatch"
        return f"ds={meta['ds']}: {len(df)} rows, verdict={meta['verdict']}"

    validate_partition(rebuild_partition(resolve_zone()))


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False,
     tags=["alethe", "poc", "naive"])
def naive_reconstruct():
    """The ungated baseline: what every engine does today."""

    @task
    def rebuild_blindly(logical_date=None):
        from deltalake import DeltaTable
        root = _root()
        dt = DeltaTable(str(root / "data" / "fct_orders"))
        dt.load_as_version(logical_date)   # resolves to vacuumed version...
        return dt.to_pyarrow_table().num_rows  # ...and explodes here

    rebuild_blindly()


reconstruct_revenue()
naive_reconstruct()

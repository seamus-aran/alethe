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

"""Honest reconstruction: an Airflow backfill gated by the PIT report.

`airflow dags backfill reconstruct_revenue -s 2024-01-01 -e 2024-06-30`
rebuilds one partition per logical date. Each run:

  CERTAIN       -> rebuilt exactly (sources time-travel to the date)
  BOUNDED       -> rebuilt with pit_verdict='BOUNDED' stamped per row
  UNACHIEVABLE  -> AirflowSkipException — visibly absent, never fabricated

The grid view becomes an honesty map of the reconstruction.

Executable walkthrough: notebooks/05_airflow_reconstruction.ipynb.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

WATERMARKS = "s3://audit/watermarks.jsonl"
DBT_MANIFEST = "target/manifest.json"
DBT_COMPILED = "target/compiled/analytics"
MODEL = "revenue_summary"


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=True,
     tags=["alethe", "reconstruction"])
def reconstruct_revenue():

    @task
    def rebuild_partition(logical_date=None):
        import alethe
        from alethe import PitStatus
        from alethe.integrations import DbtLineage

        chains = alethe.load_watermarks(WATERMARKS)
        lineage = DbtLineage(DBT_MANIFEST)
        wms = lineage.resolve_watermarks(MODEL, chains)
        report = lineage.pit_report(MODEL, watermarks=wms)

        zone = report.query(logical_date)
        if zone.status == PitStatus.UNACHIEVABLE:
            raise AirflowSkipException(
                f"{logical_date}: {zone.limiting_chains} did not exist — "
                "the population is unknowable; refusing to fabricate.")

        res = lineage.rewrite_model(
            MODEL, logical_date,
            watermarks=wms,
            compiled_path=DBT_COMPILED,
            dialect="spark",
        )
        for w in res.warnings:
            print(f"alethe: {w}")

        # Replace with your engine hook (SparkSubmitOperator, Databricks
        # SQL, etc.). pit_verdict travels with the data so BOUNDED rows
        # stay distinguishable from facts downstream.
        from your_platform import spark  # noqa: F401 — placeholder
        spark.sql(f"""
            INSERT OVERWRITE reporting.revenue_daily
            PARTITION (ds = '{logical_date:%Y-%m-%d}')
            SELECT *, '{zone.status.value}' AS pit_verdict
            FROM ({res.sql})
        """)

    rebuild_partition()


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["alethe", "witness"])
def witness_heartbeat():
    """Mint fresh watermarks daily. Also detects boundary rewinds —
    watermarks are monotone (spec §4); one moving backwards means the
    table or manifest was tampered with."""

    @task
    def mint():
        import alethe
        wm = alethe.watermark("s3://lake/orders")
        alethe.record(wm, WATERMARKS)

    mint()


reconstruct_revenue()
witness_heartbeat()

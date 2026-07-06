"""The Dagster code location: assets + job + Ray resource (+ a schedule).

    dagster dev -m pipeline.definitions      # http://localhost:3000
"""
from __future__ import annotations

import dagster as dg

from pipeline import assets
from pipeline.resources import RayResource

all_assets = dg.load_assets_from_modules([assets])

enrich_job = dg.define_asset_job("enrich_job", selection="*")

# Micro-batch mode: latency-sensitive customers, every 5 min, 15-min SLA.
# Off by default so `Materialize all` stays the manual entry point; flip it
# on in the UI (or default_status=RUNNING) to run the SLA cadence.
micro_batch_schedule = dg.ScheduleDefinition(
    name="micro_batch_every_5_min",
    job=enrich_job,
    cron_schedule="*/5 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=all_assets,
    jobs=[enrich_job],
    schedules=[micro_batch_schedule],
    resources={"ray_cluster": RayResource()},
)

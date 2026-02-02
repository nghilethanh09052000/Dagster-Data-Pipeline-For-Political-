"""
Michigan Campaign Finance Reporting definitions.

This module defines constants and configuration settings for the Michigan
contributions pipeline that extracts data from the Michigan Board of Elections (BOE)
Campaign Finance Reporting (CFR) system.
"""

import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    mi_fetch_contributions,
    mi_fetch_legacy_contribution_files,
    mi_insert_contributions,
    mi_insert_legacy_contributions_to_landing_table,
)

mi_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:michigan"
)
mi_asset_selection = dg.AssetSelection.assets(
    mi_fetch_contributions,
    mi_insert_contributions,
)
mi_legacy_asset_selection = dg.AssetSelection.assets(
    mi_insert_legacy_contributions_to_landing_table,
    mi_fetch_legacy_contribution_files,
)

# Combine both selection
mi_daily_refresh_selection = mi_asset_selection | mi_dbt_selection

# Create a Dagster job for Michigan campaign finance data
mi_refresh_job = dg.define_asset_job(
    name="mi_refresh_job",
    selection=mi_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

mi_legacy_refresh_job = dg.define_asset_job(
    name="mi_legacy_refresh_job",
    selection=mi_legacy_asset_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

# Create a schedule for the job to run daily
mi_contributions_schedule = dg.build_schedule_from_partitioned_job(
    job=mi_refresh_job,
    hour_of_day=17,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# Define the Dagster definitions
defs = dg.Definitions(
    assets=[
        mi_fetch_legacy_contribution_files,
        mi_insert_legacy_contributions_to_landing_table,
        mi_fetch_contributions,
        mi_insert_contributions,
    ],
    jobs=[mi_refresh_job, mi_legacy_refresh_job],
    schedules=[mi_contributions_schedule],
)

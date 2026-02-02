import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import sc_assets

sc_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:south_carolina"
)
sc_asset_selection = dg.AssetSelection.assets(*sc_assets)

# Combine both selection
sc_daily_refresh_selection = sc_asset_selection | sc_dbt_selection


# Create a job that processes all South Carolina contributions data
sc_refresh_job = dg.define_asset_job(
    "sc_refresh_job",
    selection=sc_daily_refresh_selection,  # All SC assets
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

# Schedule to refresh daily
# Based on the South Carolina Ethics Commission data availability
sc_daily_schedule = dg.ScheduleDefinition(
    name="sc_daily_schedule",
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    job=sc_refresh_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=sc_assets,
    jobs=[sc_refresh_job],
    schedules=[sc_daily_schedule],
)

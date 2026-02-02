import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

il_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:illinois"
)
il_asset_selection = dg.AssetSelection.assets(*assets)

# Combine both selection
il_daily_refresh_selection = il_asset_selection | il_dbt_selection

il_refresh_job = dg.define_asset_job(
    "il_refresh_job",
    selection=il_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


il_daily_refresh_schedule = dg.ScheduleDefinition(
    job=il_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=assets,
    jobs=[il_refresh_job],
    schedules=[il_daily_refresh_schedule],
)

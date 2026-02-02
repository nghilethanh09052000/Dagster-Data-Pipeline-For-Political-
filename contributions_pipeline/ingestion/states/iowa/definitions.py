import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

ia_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:iowa"
)

ia_asset_selection = dg.AssetSelection.keys(
    *[key for asset_def in assets for key in asset_def.keys]
)

ia_daily_refresh_selection = ia_dbt_selection | ia_asset_selection

ia_refresh_job = dg.define_asset_job(
    "ia_refresh_job",
    selection=ia_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

ia_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ia_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=assets,
    jobs=[ia_refresh_job],
    schedules=[ia_daily_refresh_schedule],
)

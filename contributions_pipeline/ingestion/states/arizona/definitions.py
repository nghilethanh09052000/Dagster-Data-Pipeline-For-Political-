import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

az_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:arizona"
)

az_asset_selection = dg.AssetSelection.keys(
    *[key for asset_def in assets for key in asset_def.keys]
)


az_daily_refresh_selection = az_asset_selection | az_dbt_selection

az_refresh_job = dg.define_asset_job(
    "az_refresh_job",
    selection=az_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=az_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def az_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=assets,
    jobs=[az_refresh_job],
    schedules=[az_daily_refresh_schedule],
)

import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

ar_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:arkansas"
)

ar_asset_selection = dg.AssetSelection.keys(
    *[key for asset_def in assets for key in asset_def.keys]
)
ar_daily_refresh_selection = ar_asset_selection | ar_dbt_selection

ar_refresh_job = dg.define_asset_job(
    "ar_refresh_job",
    selection=ar_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=ar_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def ar_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=assets,
    jobs=[ar_refresh_job],
    schedules=[ar_daily_refresh_schedule],
)

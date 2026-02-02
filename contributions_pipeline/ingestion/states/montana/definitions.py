import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

mt_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:montana"
)

mt_asset_selection = dg.AssetSelection.keys(
    *[key for asset_def in assets for key in asset_def.keys]
)

mt_daily_refresh_selection = mt_asset_selection | mt_dbt_selection

mt_refresh_job = dg.define_asset_job(
    "mt_refresh_job",
    selection=mt_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=mt_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def mt_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=assets,
    jobs=[mt_refresh_job],
    schedules=[mt_daily_refresh_schedule],
)

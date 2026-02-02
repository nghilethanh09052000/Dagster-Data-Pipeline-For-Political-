import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

nh_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:new_hampshire"
)
nh_asset_selection = dg.AssetSelection.assets(*assets)

# Combine both selection
nh_daily_refresh_selection = nh_asset_selection | nh_dbt_selection

nh_refresh_job = dg.define_asset_job(
    "nh_refresh_job",
    selection=nh_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    cron_schedule=r"0 17 * * *",
    job=nh_refresh_job,
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def nh_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    current_partition_key = context.scheduled_execution_time.strftime("%Y")

    return dg.RunRequest(partition_key=current_partition_key)


defs = dg.Definitions(
    assets=assets,
    jobs=[nh_refresh_job],
    schedules=[nh_daily_refresh_schedule],
)

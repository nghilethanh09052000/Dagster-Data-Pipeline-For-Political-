import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import nc_insert_transactions_to_landing, nc_transactions

nc_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:north_carolina"
)

nc_asset_selection = dg.AssetSelection.assets(
    nc_insert_transactions_to_landing, nc_transactions
)

nc_daily_refresh_selection = nc_asset_selection | nc_dbt_selection

nc_refresh_job = dg.define_asset_job(
    "nc_refresh_job",
    selection=nc_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=nc_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def nc_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[nc_transactions, nc_insert_transactions_to_landing],
    jobs=[nc_refresh_job],
    schedules=[nc_daily_refresh_schedule],
)

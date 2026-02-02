import dagster as dg

from .assets import fl_assets, fl_refresh_assets_selection

# Create a single job that processes all years
fl_refresh_job = dg.define_asset_job(
    "fl_refresh_job",
    # All FL assets, which includes every year. DBT transform as well
    selection=fl_refresh_assets_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    job=fl_refresh_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def fl_refresh_job_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(
        partition_key=context.scheduled_execution_time.strftime("%Y-%m")
    )


defs = dg.Definitions(
    assets=fl_assets,
    jobs=[fl_refresh_job],
    schedules=[fl_refresh_job_schedule],
)

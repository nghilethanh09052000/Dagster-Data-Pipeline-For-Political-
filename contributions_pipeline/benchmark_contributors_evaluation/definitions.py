import dagster as dg

from .assets import assets

benchmark_contributors_evaluation_job = dg.define_asset_job(
    "benchmark_contributors_evaluation_job",
    selection=assets,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

benchmark_contributors_evaluation_schedule = dg.ScheduleDefinition(
    job=benchmark_contributors_evaluation_job,
    cron_schedule=r"0 2 * * *",  # Daily at 2 AM
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=assets,
    jobs=[benchmark_contributors_evaluation_job],
    schedules=[benchmark_contributors_evaluation_schedule],
)

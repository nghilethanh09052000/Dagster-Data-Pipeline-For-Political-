import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ga_contributions_and_loans,
    ga_contributions_and_loans_insert_to_landing_table,
    ga_expenditures,
    ga_expenditures_insert_to_landing_table,
)

ga_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:georgia"
)

ga_asset_selection = dg.AssetSelection.assets(
    ga_contributions_and_loans,
    ga_contributions_and_loans_insert_to_landing_table,
    ga_expenditures,
    ga_expenditures_insert_to_landing_table,
)

ga_daily_refresh_selection = ga_asset_selection | ga_dbt_selection


ga_refresh_job = dg.define_asset_job(
    "ga_refresh_job",
    selection=ga_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

ga_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ga_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ga_contributions_and_loans,
        ga_contributions_and_loans_insert_to_landing_table,
        ga_expenditures,
        ga_expenditures_insert_to_landing_table,
    ],
    jobs=[ga_refresh_job],
    schedules=[ga_daily_refresh_schedule],
)

import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    co_contributions,
    co_contributions_insert_to_landing_table,
    co_expenditures,
    co_expenditures_insert_to_landing_table,
    co_loans,
    co_loans_insert_to_landing_table,
)

co_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:colorado"
)
co_asset_selection = dg.AssetSelection.assets(
    co_contributions,
    co_contributions_insert_to_landing_table,
    co_expenditures,
    co_expenditures_insert_to_landing_table,
    co_loans,
    co_loans_insert_to_landing_table,
)

# Combine both selection
co_yearly_refresh_selection = co_asset_selection | co_dbt_selection

co_yearly_refresh_job = dg.define_asset_job(
    "co_yearly_refresh_job",
    selection=co_yearly_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)


@dg.schedule(
    job=co_yearly_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def co_yearly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[
        co_contributions,
        co_contributions_insert_to_landing_table,
        co_expenditures,
        co_expenditures_insert_to_landing_table,
        co_loans,
        co_loans_insert_to_landing_table,
    ],
    jobs=[co_yearly_refresh_job],
    schedules=[co_yearly_refresh_schedule],
)

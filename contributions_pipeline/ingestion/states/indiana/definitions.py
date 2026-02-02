import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    in_fetch_all_campaign_data_full_export,
    in_insert_committee_data_to_landing,
    in_insert_contribution_data_to_landing,
    in_insert_expenditure_data_to_landing,
)

in_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:indiana"
)

in_asset_selection = dg.AssetSelection.assets(
    in_fetch_all_campaign_data_full_export,
    in_insert_committee_data_to_landing,
    in_insert_contribution_data_to_landing,
    in_insert_expenditure_data_to_landing,
)

in_daily_refresh_selection = in_asset_selection | in_dbt_selection

in_daily_refresh_job = dg.define_asset_job(
    "in_daily_refresh_job",
    selection=in_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

in_daily_refresh_schedule = dg.ScheduleDefinition(
    job=in_daily_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        in_fetch_all_campaign_data_full_export,
        in_insert_contribution_data_to_landing,
        in_insert_expenditure_data_to_landing,
        in_insert_committee_data_to_landing,
    ],
    jobs=[in_daily_refresh_job],
    schedules=[in_daily_refresh_schedule],
)

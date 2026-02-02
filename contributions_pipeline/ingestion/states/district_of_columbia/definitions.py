import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    dc_fetch_all_campaign_data_full_export,
    dc_fetch_committees_data,
    dc_insert_committees_to_landing,
    dc_insert_financial_contrib_to_landing,
    dc_insert_financial_expenditures_to_landing,
)

dc_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:district_of_columbia"
)


dc_asset_selection = dg.AssetSelection.assets(
    dc_fetch_all_campaign_data_full_export,
    dc_fetch_committees_data,
    dc_insert_financial_contrib_to_landing,
    dc_insert_financial_expenditures_to_landing,
    dc_insert_committees_to_landing,
)

dc_daily_refresh_selection = dc_asset_selection | dc_dbt_selection

dc_refresh_job = dg.define_asset_job(
    "dc_refresh_job",
    selection=dc_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

dc_daily_refresh_schedule = dg.ScheduleDefinition(
    job=dc_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        dc_fetch_all_campaign_data_full_export,
        dc_fetch_committees_data,
        dc_insert_financial_contrib_to_landing,
        dc_insert_financial_expenditures_to_landing,
        dc_insert_committees_to_landing,
    ],
    jobs=[dc_refresh_job],
    schedules=[dc_daily_refresh_schedule],
)

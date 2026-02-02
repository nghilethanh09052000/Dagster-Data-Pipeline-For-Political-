import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ut_fetch_all_campaign_data_full_export,
    ut_insert_report_data_to_landing,
)

ut_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:utah"
)
ut_asset_selection = dg.AssetSelection.assets(
    ut_fetch_all_campaign_data_full_export,
    ut_insert_report_data_to_landing,
)

# Combine both selection
ut_daily_refresh_selection = ut_asset_selection | ut_dbt_selection


ut_daily_refresh_job = dg.define_asset_job(
    "ut_daily_refresh_job",
    selection=ut_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

ut_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ut_daily_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        ut_fetch_all_campaign_data_full_export,
        ut_insert_report_data_to_landing,
    ],
    jobs=[ut_daily_refresh_job],
    schedules=[ut_daily_refresh_schedule],
)

import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    mn_fetch_all_campaign_data_full_export,
    mn_insert_contrib_to_landing,
    mn_insert_general_expenditures_to_landing,
    mn_insert_independent_expenditures_to_landing,
)

mn_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:minnesota"
)

mn_asset_selection = dg.AssetSelection.assets(
    mn_fetch_all_campaign_data_full_export,
    mn_insert_contrib_to_landing,
    mn_insert_general_expenditures_to_landing,
    mn_insert_independent_expenditures_to_landing,
)

mn_daily_refresh_selection = mn_asset_selection | mn_dbt_selection


mn_refresh_job = dg.define_asset_job(
    "mn_refresh_job",
    selection=mn_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

mn_daily_refresh_schedule = dg.ScheduleDefinition(
    job=mn_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        mn_fetch_all_campaign_data_full_export,
        mn_insert_contrib_to_landing,
        mn_insert_general_expenditures_to_landing,
        mn_insert_independent_expenditures_to_landing,
    ],
    jobs=[mn_refresh_job],
    schedules=[mn_daily_refresh_schedule],
)

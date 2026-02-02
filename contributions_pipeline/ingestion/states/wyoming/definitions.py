import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    wy_fetch_all_campaign_data_full_export,
    wy_insert_contributions_to_landing,
    wy_insert_expenditures_to_landing,
)

wy_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:wyoming"
)
wy_asset_selection = dg.AssetSelection.assets(
    wy_fetch_all_campaign_data_full_export,
    wy_insert_contributions_to_landing,
    wy_insert_expenditures_to_landing,
)

# Combine both selection
wy_daily_refresh_selection = wy_asset_selection | wy_dbt_selection

wy_daily_refresh_job = dg.define_asset_job(
    "wy_daily_refresh_job",
    selection=wy_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

wy_daily_refresh_schedule = dg.ScheduleDefinition(
    job=wy_daily_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        wy_fetch_all_campaign_data_full_export,
        wy_insert_contributions_to_landing,
        wy_insert_expenditures_to_landing,
    ],
    jobs=[wy_daily_refresh_job],
    schedules=[wy_daily_refresh_schedule],
)

import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ms_candidate_committee,
    ms_contributions,
    ms_expenditures,
    ms_insert_candidate_committee,
    ms_insert_contributions,
    ms_insert_expenditures,
)

ms_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:mississippi"
)
ms_asset_selection = dg.AssetSelection.assets(
    ms_candidate_committee,
    ms_contributions,
    ms_expenditures,
    ms_insert_candidate_committee,
    ms_insert_contributions,
    ms_insert_expenditures,
)

ms_daily_refresh_selection = ms_dbt_selection | ms_asset_selection

ms_refresh_job = dg.define_asset_job(
    "ms_refresh_job",
    selection=ms_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

ms_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ms_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ms_contributions,
        ms_insert_contributions,
        ms_expenditures,
        ms_insert_expenditures,
        ms_candidate_committee,
        ms_insert_candidate_committee,
    ],
    jobs=[ms_refresh_job],
    schedules=[ms_daily_refresh_schedule],
)

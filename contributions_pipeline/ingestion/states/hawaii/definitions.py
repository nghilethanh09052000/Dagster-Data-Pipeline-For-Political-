import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    hi_fetch_all_campaign_data_full_export,
    hi_insert_contrib_made_to_candidate_to_landing,
    hi_insert_contrib_recv_by_candidate_to_landing,
    hi_insert_contrib_recv_by_non_candidate_to_landing,
    hi_insert_expenditure_made_by_candidate_to_landing,
    hi_insert_expenditure_made_by_non_candidate_to_landing,
    hi_insert_loans_recv_by_candidate_to_landing,
)

hi_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:hawaii"
)

hi_asset_selection = dg.AssetSelection.assets(
    hi_fetch_all_campaign_data_full_export,
    hi_insert_contrib_made_to_candidate_to_landing,
    hi_insert_contrib_recv_by_candidate_to_landing,
    hi_insert_contrib_recv_by_non_candidate_to_landing,
    hi_insert_expenditure_made_by_candidate_to_landing,
    hi_insert_expenditure_made_by_non_candidate_to_landing,
    hi_insert_loans_recv_by_candidate_to_landing,
)

hi_daily_refresh_selection = hi_dbt_selection | hi_asset_selection

hi_refresh_job = dg.define_asset_job(
    "hi_refresh_job",
    selection=hi_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

hi_daily_refresh_schedule = dg.ScheduleDefinition(
    job=hi_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        hi_fetch_all_campaign_data_full_export,
        hi_insert_contrib_made_to_candidate_to_landing,
        hi_insert_contrib_recv_by_candidate_to_landing,
        hi_insert_contrib_recv_by_non_candidate_to_landing,
        hi_insert_expenditure_made_by_candidate_to_landing,
        hi_insert_expenditure_made_by_non_candidate_to_landing,
        hi_insert_loans_recv_by_candidate_to_landing,
    ],
    jobs=[hi_refresh_job],
    schedules=[hi_daily_refresh_schedule],
)

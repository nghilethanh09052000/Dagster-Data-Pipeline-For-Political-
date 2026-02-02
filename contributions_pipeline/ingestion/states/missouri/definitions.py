import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    mo_fetch_all_campaign_data_full_export,
    mo_insert_cd1_a_to_landing,
    mo_insert_cd1_b_to_landing,
    mo_insert_cd1_c_to_landing,
    mo_insert_cd1a_to_landing,
    mo_insert_cd1b1_a_to_landing,
    mo_insert_cd1b2_a_to_landing,
    mo_insert_cd3_a_to_landing,
    mo_insert_cd3_b_to_landing,
    mo_insert_cd3_c_to_landing,
    mo_insert_committee_data_to_landing,
    mo_insert_summary_to_landing,
)

mo_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:missouri"
)

mo_asset_selection = dg.AssetSelection.assets(
    mo_fetch_all_campaign_data_full_export,
    mo_insert_cd1_a_to_landing,
    mo_insert_cd1_b_to_landing,
    mo_insert_cd1_c_to_landing,
    mo_insert_cd1a_to_landing,
    mo_insert_cd1b1_a_to_landing,
    mo_insert_cd1b2_a_to_landing,
    mo_insert_cd3_a_to_landing,
    mo_insert_cd3_b_to_landing,
    mo_insert_cd3_c_to_landing,
    mo_insert_committee_data_to_landing,
    mo_insert_summary_to_landing,
)

mo_daily_refresh_selection = mo_asset_selection | mo_dbt_selection

mo_refresh_job = dg.define_asset_job(
    "mo_refresh_job",
    selection=mo_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

mo_daily_refresh_schedule = dg.ScheduleDefinition(
    job=mo_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        mo_fetch_all_campaign_data_full_export,
        mo_insert_cd1_a_to_landing,
        mo_insert_cd1_b_to_landing,
        mo_insert_cd1_c_to_landing,
        mo_insert_cd1a_to_landing,
        mo_insert_cd1b1_a_to_landing,
        mo_insert_cd1b2_a_to_landing,
        mo_insert_cd3_a_to_landing,
        mo_insert_cd3_b_to_landing,
        mo_insert_cd3_c_to_landing,
        mo_insert_summary_to_landing,
        mo_insert_committee_data_to_landing,
    ],
    jobs=[mo_refresh_job],
    schedules=[mo_daily_refresh_schedule],
)

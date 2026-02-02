import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    vt_fetch_campaign_data,
    vt_fetch_candidates_data,
    vt_fetch_committee_data,
    vt_insert_candidates_to_landing_table,
    vt_insert_committees_to_landing_table,
    vt_insert_contributions_to_landing_table,
    vt_insert_expenditures_to_landing_table,
)

vt_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:vermont"
)

vt_asset_selection = dg.AssetSelection.assets(
    vt_fetch_candidates_data,
    vt_fetch_committee_data,
    vt_fetch_campaign_data,
    vt_insert_contributions_to_landing_table,
    vt_insert_expenditures_to_landing_table,
    vt_insert_candidates_to_landing_table,
    vt_insert_committees_to_landing_table,
)

vt_daily_refresh_selection = vt_asset_selection | vt_dbt_selection

vt_refresh_job = dg.define_asset_job(
    "vt_refresh_job",
    selection=vt_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)


@dg.schedule(
    job=vt_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def vt_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[
        vt_fetch_candidates_data,
        vt_fetch_committee_data,
        vt_fetch_campaign_data,
        vt_insert_contributions_to_landing_table,
        vt_insert_expenditures_to_landing_table,
        vt_insert_candidates_to_landing_table,
        vt_insert_committees_to_landing_table,
    ],
    jobs=[vt_refresh_job],
    schedules=[vt_daily_refresh_schedule],
)

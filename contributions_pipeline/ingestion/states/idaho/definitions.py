import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    id_fetch_new_idaho_campaign_finance,
    id_fetch_old_idaho_campaign_finance,
    id_insert_new_contributions_to_landing_table,
    id_insert_old_candidate_contributions_to_landing_table,
    id_insert_old_committee_contributions_to_landing_table,
)

id_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:idaho"
)
id_asset_selection = dg.AssetSelection.assets(
    id_fetch_new_idaho_campaign_finance,
    id_fetch_old_idaho_campaign_finance,
    id_insert_new_contributions_to_landing_table,
    id_insert_old_candidate_contributions_to_landing_table,
    id_insert_old_committee_contributions_to_landing_table,
)

# Combine both selection
id_daily_refresh_selection = id_asset_selection | id_dbt_selection

id_refresh_job = dg.define_asset_job(
    "id_refresh_job",
    selection=id_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

id_daily_refresh_schedule = dg.ScheduleDefinition(
    job=id_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        id_fetch_new_idaho_campaign_finance,
        id_fetch_old_idaho_campaign_finance,
        id_insert_new_contributions_to_landing_table,
        id_insert_old_candidate_contributions_to_landing_table,
        id_insert_old_committee_contributions_to_landing_table,
    ],
    jobs=[id_refresh_job],
    schedules=[id_daily_refresh_schedule],
)

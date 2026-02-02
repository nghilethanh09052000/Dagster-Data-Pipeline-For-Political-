import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    fec_candidate_master,
    fec_candidate_master_insert_to_landing_table,
    fec_committee_master,
    fec_committee_master_insert_to_landing_table,
    fec_daily_schedule_a,
    fec_daily_schedule_a_insert_to_landing_table,
)

# Get all DBT transformations with "federal" tag, also run all downstream transformation
# from those assets
fec_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:federal"
)
# All the scraping and landing table insertion assets
fec_asset_selection = dg.AssetSelection.assets(
    fec_committee_master,
    fec_committee_master_insert_to_landing_table,
    fec_candidate_master,
    fec_candidate_master_insert_to_landing_table,
)

# Combine both selection
fec_daily_refresh_selection = fec_asset_selection | fec_dbt_selection

fec_refresh_job = dg.define_asset_job(
    "fec_refresh_job",
    selection=fec_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

fec_daily_refresh_schedule = dg.ScheduleDefinition(
    job=fec_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

fec_paritioned_schedule_a_asset_selection = dg.AssetSelection.assets(
    fec_daily_schedule_a,
    fec_daily_schedule_a_insert_to_landing_table,
)

fec_partitioned_schdule_a_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="federal_individual_contributions_transform"
)

fec_paritioned_asset_selection_with_dbt = (
    fec_paritioned_schedule_a_asset_selection
    | fec_paritioned_schedule_a_asset_selection
)

fec_daily_partitioned_schedule_a_job = dg.define_asset_job(
    "fec_daily_partitioned_schedule_a_job",
    selection=fec_paritioned_asset_selection_with_dbt,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

fec_daily_partitioned_schedule_a_job_schedule = dg.build_schedule_from_partitioned_job(
    job=fec_daily_partitioned_schedule_a_job,
    hour_of_day=17,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        fec_daily_schedule_a,
        fec_daily_schedule_a_insert_to_landing_table,
        fec_committee_master,
        fec_committee_master_insert_to_landing_table,
        fec_candidate_master,
        fec_candidate_master_insert_to_landing_table,
    ],
    jobs=[fec_refresh_job, fec_daily_partitioned_schedule_a_job],
    schedules=[
        fec_daily_refresh_schedule,
        fec_daily_partitioned_schedule_a_job_schedule,
    ],
)

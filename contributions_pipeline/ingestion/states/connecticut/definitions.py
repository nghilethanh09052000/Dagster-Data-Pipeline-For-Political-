import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ct_fetch_committee_candidate_assets,
    ct_fetch_committee_exploratoy_assets,
    ct_fetch_committee_party_assets,
    ct_fetch_committee_politicial_assets,
    ct_fetch_contributions_candidate_data,
    ct_fetch_contributions_exploratory_data,
    ct_fetch_contributions_party_data,
    ct_fetch_contributions_political_data,
    ct_inserting_committees_reports_data,
    ct_inserting_contributions_reports_data,
    ct_schedule_static_partition,
)

# --- DBT asset selection ---
ct_dbt_committees_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="connecticut_committee_master_transform"
)

ct_dbt_contributions_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="connecticut_individual_contributions_transform"
)


# --- Group assets by partition definition ---
ct_daily_asset_selection = dg.AssetSelection.assets(
    ct_fetch_contributions_candidate_data,
    ct_fetch_contributions_exploratory_data,
    ct_fetch_contributions_party_data,
    ct_fetch_contributions_political_data,
    ct_inserting_contributions_reports_data,
)

ct_static_asset_selection = dg.AssetSelection.assets(
    ct_fetch_committee_candidate_assets,
    ct_fetch_committee_exploratoy_assets,
    ct_fetch_committee_party_assets,
    ct_fetch_committee_politicial_assets,
    ct_inserting_committees_reports_data,
)

# --- Daily refresh job (for contributions data) ---
ct_daily_partitioned_refresh_job = dg.define_asset_job(
    name="ct_daily_partitioned_refresh_job",
    selection=ct_daily_asset_selection | ct_dbt_contributions_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=ct_daily_partitioned_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def ct_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(
        partition_key=context.scheduled_execution_time.strftime("%m/%d/%Y")
    )


# --- Static refresh job (for committees data by letter) ---
ct_static_refresh_job = dg.define_asset_job(
    name="ct_static_refresh_job",
    selection=ct_static_asset_selection | ct_dbt_committees_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=ct_static_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def ct_static_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return [
        dg.RunRequest(
            run_key=context.scheduled_execution_time.isoformat(timespec="minutes")
            + f"-{letter}",
            partition_key=letter,
        )
        for letter in ct_schedule_static_partition.get_partition_keys()
    ]


# --- Export definitions ---
defs = dg.Definitions(
    assets=[
        ct_fetch_committee_candidate_assets,
        ct_fetch_committee_exploratoy_assets,
        ct_fetch_committee_party_assets,
        ct_fetch_committee_politicial_assets,
        ct_inserting_committees_reports_data,
        ct_fetch_contributions_candidate_data,
        ct_fetch_contributions_exploratory_data,
        ct_fetch_contributions_party_data,
        ct_fetch_contributions_political_data,
        ct_inserting_contributions_reports_data,
    ],
    jobs=[
        ct_daily_partitioned_refresh_job,
        ct_static_refresh_job,
    ],
    schedules=[
        ct_daily_refresh_schedule,
        ct_static_refresh_schedule,
    ],
)

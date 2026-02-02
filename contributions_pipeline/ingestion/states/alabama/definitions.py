import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    # Static partitioned assets (candidates/committees data)
    al_fetch_candidates_data,
    # Yearly partitioned assets (contributions data)
    al_fetch_contributions_bulk_raw_data,
    al_fetch_political_action_committee_data,
    al_insert_candidates_to_landing_table,
    al_insert_cash_contributions_to_landing_table,
    al_insert_expenditures_to_landing_table,
    al_insert_in_kind_contributions_to_landing_table,
    al_insert_other_receipts_to_landing_table,
    al_insert_political_action_committee_to_landing_table,
    al_schedule_static_status_partition,
)

# --- DBT asset selection ---
al_dbt_contributions_selection = build_dbt_asset_selection(
    [dbt_transformations],
    dbt_select="alabama_individual_contributions_transform",
)

al_dbt_committees_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="alabama_committee_master_transform"
)

al_dbt_candidates_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="alabama_candidate_master_transform"
)


# --- Group assets by partition definition ---
al_yearly_asset_selection = dg.AssetSelection.assets(
    al_fetch_contributions_bulk_raw_data,
    al_insert_cash_contributions_to_landing_table,
    al_insert_expenditures_to_landing_table,
    al_insert_in_kind_contributions_to_landing_table,
    al_insert_other_receipts_to_landing_table,
)

al_static_status_asset_selection = dg.AssetSelection.assets(
    al_fetch_candidates_data,
    al_insert_candidates_to_landing_table,
    al_fetch_political_action_committee_data,
    al_insert_political_action_committee_to_landing_table,
)


# --- Yearly refresh job (for contributions data) ---
al_yearly_partitioned_refresh_job = dg.define_asset_job(
    name="al_yearly_partitioned_refresh_job",
    selection=al_yearly_asset_selection | al_dbt_contributions_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=al_yearly_partitioned_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def al_yearly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


# --- Static refresh job (for candidates/committees data by status) ---
al_static_status_refresh_job = dg.define_asset_job(
    name="al_static_status_refresh_job",
    selection=(
        al_static_status_asset_selection
        | al_dbt_candidates_selection
        | al_dbt_committees_selection
    ),
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=al_static_status_refresh_job,
    cron_schedule="0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def al_static_status_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return [
        dg.RunRequest(
            run_key=context.scheduled_execution_time.isoformat(timespec="minutes")
            + f"-{status}",
            partition_key=status,
        )
        for status in al_schedule_static_status_partition.get_partition_keys()
    ]


# --- Export definitions ---
defs = dg.Definitions(
    assets=[
        # Yearly partitioned assets
        al_fetch_contributions_bulk_raw_data,
        al_insert_cash_contributions_to_landing_table,
        al_insert_expenditures_to_landing_table,
        al_insert_in_kind_contributions_to_landing_table,
        al_insert_other_receipts_to_landing_table,
        # Static partitioned assets
        al_fetch_candidates_data,
        al_insert_candidates_to_landing_table,
        al_fetch_political_action_committee_data,
        al_insert_political_action_committee_to_landing_table,
    ],
    jobs=[al_yearly_partitioned_refresh_job, al_static_status_refresh_job],
    schedules=[al_yearly_refresh_schedule, al_static_status_refresh_schedule],
)

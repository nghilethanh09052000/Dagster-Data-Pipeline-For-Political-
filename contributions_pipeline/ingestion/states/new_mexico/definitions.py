import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    nm_fetch_candidates_raw_data,
    nm_fetch_committees_raw_data,
    nm_fetch_contributions_raw_data,
    nm_fetch_expenditures_raw_data,
    nm_fetch_transactions_candidate_raw_data,
    nm_fetch_transactions_pac_raw_data,
    nm_insert_candidates_to_landing_table,
    nm_insert_committees_to_landing_table,
    nm_insert_raw_contributions_to_landing_table,
    nm_insert_raw_expenditures_to_landing_table,
    nm_insert_transactions_candidate_to_landing_table,
    nm_insert_transactions_pac_to_landing_table,
)

nm_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:new_mexico"
)

# All assets selection
nm_asset_selection = dg.AssetSelection.assets(
    nm_fetch_candidates_raw_data,
    nm_fetch_committees_raw_data,
    nm_fetch_transactions_candidate_raw_data,
    nm_fetch_transactions_pac_raw_data,
    nm_fetch_contributions_raw_data,
    nm_fetch_expenditures_raw_data,
    nm_insert_candidates_to_landing_table,
    nm_insert_committees_to_landing_table,
    nm_insert_transactions_candidate_to_landing_table,
    nm_insert_transactions_pac_to_landing_table,
    nm_insert_raw_contributions_to_landing_table,
    nm_insert_raw_expenditures_to_landing_table,
)

# Yearly refresh selection for all assets
nm_yearly_refresh_selection = nm_asset_selection | nm_dbt_selection

# Job definitions
nm_yearly_refresh_job = dg.define_asset_job(
    "nm_yearly_refresh_job",
    selection=nm_yearly_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
    description="Yearly refresh job for all New Mexico campaign finance assets",
)


# Schedule definitions
@dg.schedule(
    job=nm_yearly_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    name="nm_yearly_refresh_schedule",
    description="Yearly schedule for all New Mexico campaign finance data refresh",
)
def nm_yearly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


# Main definitions
defs = dg.Definitions(
    assets=[
        nm_fetch_candidates_raw_data,
        nm_fetch_committees_raw_data,
        nm_fetch_transactions_candidate_raw_data,
        nm_fetch_transactions_pac_raw_data,
        nm_fetch_contributions_raw_data,
        nm_fetch_expenditures_raw_data,
        nm_insert_candidates_to_landing_table,
        nm_insert_committees_to_landing_table,
        nm_insert_transactions_candidate_to_landing_table,
        nm_insert_transactions_pac_to_landing_table,
        nm_insert_raw_contributions_to_landing_table,
        nm_insert_raw_expenditures_to_landing_table,
    ],
    jobs=[nm_yearly_refresh_job],
    schedules=[nm_yearly_refresh_schedule],
)

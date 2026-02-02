import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ks_get_and_save_candidate_data,
    ks_get_and_save_contribution_data,
    ks_get_and_save_expenditure_data,
    ks_get_and_save_party_politicial_committee_data,
    ks_insert_candidate_to_landing_table,
    ks_insert_committees_to_landing_table,
    ks_insert_contribution_to_landing_table,
    ks_insert_expenditure_to_landing_table,
)

ks_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:kansas"
)

ks_asset_selection = dg.AssetSelection.assets(
    ks_get_and_save_candidate_data,
    ks_get_and_save_contribution_data,
    ks_get_and_save_expenditure_data,
    ks_get_and_save_party_politicial_committee_data,
    ks_insert_candidate_to_landing_table,
    ks_insert_contribution_to_landing_table,
    ks_insert_expenditure_to_landing_table,
    ks_insert_committees_to_landing_table,
)

ks_daily_refresh_selection = ks_asset_selection | ks_dbt_selection


ks_refresh_job = dg.define_asset_job(
    "ks_refresh_job",
    selection=ks_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

ks_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ks_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ks_get_and_save_candidate_data,
        ks_get_and_save_contribution_data,
        ks_get_and_save_expenditure_data,
        ks_get_and_save_party_politicial_committee_data,
        ks_insert_candidate_to_landing_table,
        ks_insert_contribution_to_landing_table,
        ks_insert_expenditure_to_landing_table,
        ks_insert_committees_to_landing_table,
    ],
    jobs=[ks_refresh_job],
    schedules=[ks_daily_refresh_schedule],
)

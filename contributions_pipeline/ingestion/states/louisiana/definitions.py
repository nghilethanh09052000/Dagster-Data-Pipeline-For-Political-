import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    la_fetch_candidate_search_initial_page,
    la_fetch_candidates_data,
    la_fetch_contributions_data,
    la_fetch_expenditure_search_initial_page,
    la_fetch_expenditures_data,
    la_inserting_candidates_data,
    la_inserting_contributions_data,
    la_inserting_expenditures_data,
)

la_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:louisiana"
)
la_asset_selection = dg.AssetSelection.assets(
    la_fetch_candidate_search_initial_page,
    la_fetch_candidates_data,
    la_fetch_contributions_data,
    la_fetch_expenditure_search_initial_page,
    la_fetch_expenditures_data,
    la_inserting_candidates_data,
    la_inserting_contributions_data,
    la_inserting_expenditures_data,
)

la_daily_refresh_selection = la_dbt_selection | la_asset_selection

la_daily_refresh_job = dg.define_asset_job(
    "la_daily_refresh_job",
    selection=la_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

la_daily_refresh_schedule = dg.ScheduleDefinition(
    job=la_daily_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    # This was super expensive. Pause for now
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        la_fetch_contributions_data,
        la_fetch_candidates_data,
        la_fetch_candidate_search_initial_page,
        la_fetch_expenditure_search_initial_page,
        la_fetch_expenditures_data,
        la_inserting_contributions_data,
        la_inserting_expenditures_data,
        la_inserting_candidates_data,
    ],
    jobs=[la_daily_refresh_job],
    schedules=[la_daily_refresh_schedule],
)

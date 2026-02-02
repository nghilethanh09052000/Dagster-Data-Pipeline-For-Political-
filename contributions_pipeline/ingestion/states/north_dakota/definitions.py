import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    nd_fetch_candidates_data,
    nd_fetch_committees_data,
    nd_fetch_contributions_data,
    nd_fetch_expenditures_data,
    nd_get_initialize_search_session,
    nd_inserting_candidates_data,
    nd_inserting_committees_data,
    nd_inserting_contributions_data,
    nd_inserting_expenditures_data,
)

nd_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:north_dakota"
)


nd_asset_selection = dg.AssetSelection.assets(
    nd_fetch_candidates_data,
    nd_fetch_committees_data,
    nd_fetch_contributions_data,
    nd_fetch_expenditures_data,
    nd_get_initialize_search_session,
    nd_inserting_candidates_data,
    nd_inserting_committees_data,
    nd_inserting_contributions_data,
    nd_inserting_expenditures_data,
)

nd_daily_refresh_selection = nd_asset_selection | nd_dbt_selection


nd_refresh_job = dg.define_asset_job(
    "nd_refresh_job",
    selection=nd_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

nd_daily_refresh_schedule = dg.ScheduleDefinition(
    job=nd_refresh_job,
    cron_schedule="0 17 * * *",  # Run at 3 AM and 5 PM Eastern time
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        nd_get_initialize_search_session,
        nd_fetch_candidates_data,
        nd_fetch_committees_data,
        nd_fetch_contributions_data,
        nd_fetch_expenditures_data,
        nd_inserting_candidates_data,
        nd_inserting_committees_data,
        nd_inserting_contributions_data,
        nd_inserting_expenditures_data,
    ],
    jobs=[nd_refresh_job],
    schedules=[nd_daily_refresh_schedule],
)

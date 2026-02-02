import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ky_fetch_candidates_data,
    ky_fetch_contributions_data,
    ky_fetch_expenditures_data,
    ky_fetch_organizations_data,
    ky_insert_candidates_reports_data,
    ky_insert_contributions_reports_data,
    ky_insert_expenditures_reports_data,
    ky_insert_organizations_reports_data,
)

ky_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:kentucky"
)

ky_asset_selection = dg.AssetSelection.assets(
    ky_fetch_candidates_data,
    ky_fetch_contributions_data,
    ky_fetch_expenditures_data,
    ky_fetch_organizations_data,
    ky_insert_candidates_reports_data,
    ky_insert_contributions_reports_data,
    ky_insert_expenditures_reports_data,
    ky_insert_organizations_reports_data,
)

ky_daily_refresh_selection = ky_asset_selection | ky_dbt_selection


ky_refresh_job = dg.define_asset_job(
    "ky_refresh_job",
    selection=ky_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

ky_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ky_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ky_fetch_candidates_data,
        ky_fetch_contributions_data,
        ky_fetch_expenditures_data,
        ky_fetch_organizations_data,
        ky_insert_candidates_reports_data,
        ky_insert_contributions_reports_data,
        ky_insert_expenditures_reports_data,
        ky_insert_organizations_reports_data,
    ],
    jobs=[ky_refresh_job],
    schedules=[ky_daily_refresh_schedule],
)

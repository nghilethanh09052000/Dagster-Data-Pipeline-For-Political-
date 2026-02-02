import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    tn_fetch_candidates_data,
    tn_fetch_contributions_data,
    tn_fetch_expenditures_data,
    tn_inserting_candidates_reports_data,
    tn_inserting_contributions_reports_data,
    tn_inserting_expenditures_reports_data,
)

tn_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:tennessee"
)
tn_asset_selection = dg.AssetSelection.assets(
    tn_fetch_candidates_data,
    tn_fetch_contributions_data,
    tn_fetch_expenditures_data,
    tn_inserting_candidates_reports_data,
    tn_inserting_contributions_reports_data,
    tn_inserting_expenditures_reports_data,
)

# Combine both selection
tn_daily_refresh_selection = tn_asset_selection | tn_dbt_selection


tn_refresh_job = dg.define_asset_job(
    "tn_refresh_job",
    selection=tn_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

tn_daily_refresh_schedule = dg.ScheduleDefinition(
    job=tn_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        tn_fetch_candidates_data,
        tn_fetch_contributions_data,
        tn_fetch_expenditures_data,
        tn_inserting_candidates_reports_data,
        tn_inserting_contributions_reports_data,
        tn_inserting_expenditures_reports_data,
    ],
    jobs=[tn_refresh_job],
    schedules=[tn_daily_refresh_schedule],
)

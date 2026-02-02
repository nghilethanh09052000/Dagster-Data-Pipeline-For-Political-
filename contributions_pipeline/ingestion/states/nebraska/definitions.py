import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ne_contributions_loans_download_data,
    ne_expenditures_download_data,
    ne_insert_contributions_loans,
    ne_insert_expenditures,
)

ne_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:nebraska"
)
ne_asset_selection = dg.AssetSelection.assets(
    ne_contributions_loans_download_data,
    ne_expenditures_download_data,
    ne_insert_contributions_loans,
    ne_insert_expenditures,
)

# Combine both selection
ne_daily_refresh_selection = ne_asset_selection | ne_dbt_selection

ne_refresh_job = dg.define_asset_job(
    "ne_refresh_job",
    selection=ne_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

ne_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ne_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ne_contributions_loans_download_data,
        ne_expenditures_download_data,
        ne_insert_contributions_loans,
        ne_insert_expenditures,
    ],
    jobs=[ne_refresh_job],
    schedules=[ne_daily_refresh_schedule],
)

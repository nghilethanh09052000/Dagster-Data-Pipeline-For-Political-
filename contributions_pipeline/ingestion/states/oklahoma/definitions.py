import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ok_committees_download_data,
    ok_contributions_loans_download_data,
    ok_expenditures_download_data,
    ok_insert_committees,
    ok_insert_contributions_loans,
    ok_insert_expenditures,
    ok_insert_lobbyist_expenditures,
    ok_lobbyist_expenditures_download_data,
)

ok_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:oklahoma"
)

ok_asset_selection = dg.AssetSelection.assets(
    ok_contributions_loans_download_data,
    ok_expenditures_download_data,
    ok_insert_contributions_loans,
    ok_insert_expenditures,
    ok_insert_lobbyist_expenditures,
    ok_lobbyist_expenditures_download_data,
    ok_committees_download_data,
    ok_insert_committees,
)

ok_daily_refresh_selection = ok_asset_selection | ok_dbt_selection


ok_refresh_job = dg.define_asset_job(
    "ok_refresh_job",
    selection=ok_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


ok_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ok_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        ok_contributions_loans_download_data,
        ok_expenditures_download_data,
        ok_insert_contributions_loans,
        ok_insert_expenditures,
        ok_insert_lobbyist_expenditures,
        ok_lobbyist_expenditures_download_data,
        ok_committees_download_data,
        ok_insert_committees,
    ],
    jobs=[ok_refresh_job],
    schedules=[ok_daily_refresh_schedule],
)

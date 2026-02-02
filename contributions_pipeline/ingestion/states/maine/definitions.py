import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    me_contributions_and_loans,
    me_expenditures,
    me_insert_new_format_contributions_and_loans,
    me_insert_new_format_expenditures,
    me_insert_old_format_contributions_and_loans,
    me_insert_old_format_expenditures,
)

mn_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:maine"
)
mn_asset_selection = dg.AssetSelection.assets(
    me_contributions_and_loans,
    me_expenditures,
    me_insert_new_format_contributions_and_loans,
    me_insert_new_format_expenditures,
    me_insert_old_format_contributions_and_loans,
    me_insert_old_format_expenditures,
)

# Combine both selection
mn_daily_refresh_selection = mn_asset_selection | mn_dbt_selection


me_refresh_job = dg.define_asset_job(
    "me_refresh_job",
    selection=mn_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


me_daily_refresh_schedule = dg.ScheduleDefinition(
    job=me_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        me_contributions_and_loans,
        me_insert_old_format_contributions_and_loans,
        me_insert_new_format_contributions_and_loans,
        me_expenditures,
        me_insert_old_format_expenditures,
        me_insert_new_format_expenditures,
    ],
    jobs=[me_refresh_job],
    schedules=[me_daily_refresh_schedule],
)

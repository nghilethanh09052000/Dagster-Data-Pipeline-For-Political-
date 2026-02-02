import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import md_contributions_and_loans, md_insert_contributions_and_loans

md_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:maryland"
)
md_asset_selection = dg.AssetSelection.assets(
    md_contributions_and_loans, md_insert_contributions_and_loans
)

# Combine both selection
md_daily_refresh_selection = md_asset_selection | md_dbt_selection


md_refresh_job = dg.define_asset_job(
    "md_refresh_job",
    selection=md_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


md_daily_refresh_schedule = dg.ScheduleDefinition(
    job=md_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


defs = dg.Definitions(
    assets=[md_contributions_and_loans, md_insert_contributions_and_loans],
    jobs=[md_refresh_job],
    schedules=[md_daily_refresh_schedule],
)

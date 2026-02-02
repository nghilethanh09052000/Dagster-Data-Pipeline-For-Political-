import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import assets

wa_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:washington"
)
wa_asset_selection = dg.AssetSelection.assets(*assets)

# Combine both selection
wa_daily_refresh_selection = wa_asset_selection | wa_dbt_selection


wa_refresh_job = dg.define_asset_job(
    "wa_refresh_job",
    selection=wa_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

wa_daily_refresh_schedule = dg.ScheduleDefinition(
    job=wa_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=assets,
    jobs=[wa_refresh_job],
    schedules=[wa_daily_refresh_schedule],
)

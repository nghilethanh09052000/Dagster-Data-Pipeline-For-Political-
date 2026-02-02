import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from .assets import dbt_transformations
from .resource import dbt_cli_resource

master_tables_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:master_tables"
)

master_tables_update_job = dg.define_asset_job(
    "master_tables_update",
    selection=master_tables_dbt_selection,
)

master_tables_update_refresh_schedule = dg.ScheduleDefinition(
    job=master_tables_update_job,
    # +5h after execution of all states
    cron_schedule=r"0 8,22 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[dbt_transformations],
    jobs=[master_tables_update_job],
    schedules=[master_tables_update_refresh_schedule],
    resources={"dbt": dbt_cli_resource},
)

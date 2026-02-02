import dagster as dg

from .assets import assets

move_pipline_db_to_application_db = dg.define_asset_job(
    "move_pipline_db_to_application_db", selection=assets
)

move_to_app_db_schedule = dg.ScheduleDefinition(
    job=move_pipline_db_to_application_db,
    cron_schedule=r"0 3 * * *",  # Every day at 3 AM
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=assets,
    jobs=[move_pipline_db_to_application_db],
    schedules=[move_to_app_db_schedule],
)

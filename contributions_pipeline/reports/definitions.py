import dagster as dg

from .assets import contribution_source_report, send_contribution_source_report_to_slack

# Create asset selection
reports_asset_selection = dg.AssetSelection.assets(
    contribution_source_report, send_contribution_source_report_to_slack
)

# Define the job
contribution_source_report_job = dg.define_asset_job(
    "contribution_source_report_job",
    selection=reports_asset_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

# Define the schedule (every 6 hours: 00:00, 06:00, 12:00, 18:00)
contribution_source_report_schedule = dg.ScheduleDefinition(
    job=contribution_source_report_job,
    cron_schedule=r"0 1,13 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# Create definitions
defs = dg.Definitions(
    assets=[contribution_source_report, send_contribution_source_report_to_slack],
    jobs=[contribution_source_report_job],
    schedules=[contribution_source_report_schedule],
)

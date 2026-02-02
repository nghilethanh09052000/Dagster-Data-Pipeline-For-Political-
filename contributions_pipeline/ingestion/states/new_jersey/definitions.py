import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import nj_assets

nj_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:new_jersey"
)
nj_asset_selection = dg.AssetSelection.assets(*nj_assets)

# Combine both selection
nj_assets_refresh_selection = nj_asset_selection | nj_dbt_selection


# Create a single job that processes all New Jersey contributions data
nj_refresh_job = dg.define_asset_job(
    "nj_refresh_job",
    selection=nj_assets_refresh_selection,  # All NJ assets
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


# Schedule to refresh twice daily
# Based on the documentation for New Jersey data which should be refreshed regularly
@dg.schedule(
    cron_schedule="0 17 * * *",  # Twice daily at 3am and 5pm Eastern
    execution_timezone="America/New_York",
    job=nj_refresh_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def nj_refresh_job_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=nj_assets,
    jobs=[nj_refresh_job],
    schedules=[nj_refresh_job_schedule],
)

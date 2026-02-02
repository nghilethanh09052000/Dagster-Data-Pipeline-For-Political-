import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ma_contributions_monthly_partition,
    ma_fetch_all_campaign_data_full_export,
    ma_fetch_daily_contributions_data,
    ma_insert_committee_table_to_landing,
    ma_insert_contributions_to_landing,
    ma_insert_report_items_to_landing,
    ma_insert_reports_to_landing,
)

# --- DBT asset selection ---
ma_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations],
    dbt_select="tag:massachussets",
)

# --- Raw & landing assets (monthly partitioned) ---
ma_monthly_asset_selection = dg.AssetSelection.assets(
    ma_fetch_daily_contributions_data,
    ma_fetch_all_campaign_data_full_export,
    ma_insert_contributions_to_landing,
    ma_insert_committee_table_to_landing,
    ma_insert_report_items_to_landing,
    ma_insert_reports_to_landing,
)

# --- Combined refresh selection ---
ma_monthly_refresh_selection = ma_monthly_asset_selection | ma_dbt_selection

# --- Job definition ---
ma_monthly_refresh_job = dg.define_asset_job(
    name="ma_monthly_refresh_job",
    selection=ma_monthly_refresh_selection,
    partitions_def=ma_contributions_monthly_partition,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        "dagster/retry_strategy": "ALL_STEPS",
    },
)


# --- Schedule definition ---
@dg.schedule(
    job=ma_monthly_refresh_job,
    cron_schedule="0 17 1 * *",  # runs monthly on the 1st at 5 PM EST
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def ma_monthly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(
        partition_key=context.scheduled_execution_time.strftime("%Y-%m")
    )


# --- Export Dagster definitions ---
defs = dg.Definitions(
    assets=[
        ma_fetch_daily_contributions_data,
        ma_fetch_all_campaign_data_full_export,
        ma_insert_contributions_to_landing,
        ma_insert_reports_to_landing,
        ma_insert_report_items_to_landing,
        ma_insert_committee_table_to_landing,
    ],
    jobs=[ma_monthly_refresh_job],
    schedules=[ma_monthly_refresh_schedule],
)

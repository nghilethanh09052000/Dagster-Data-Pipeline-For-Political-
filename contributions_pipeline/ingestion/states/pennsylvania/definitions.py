import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    pa_fetch_all_campaign_data_full_export,
    pa_insert_new_format_contrib_to_landing,
    pa_insert_new_format_debt_to_landing,
    pa_insert_new_format_expense_to_landing,
    pa_insert_new_format_filer_to_landing,
    pa_insert_new_format_receipt_to_landing,
    pa_insert_old_format_contrib_to_landing,
    pa_insert_old_format_debt_to_landing,
    pa_insert_old_format_expense_to_landing,
    pa_insert_old_format_filer_to_landing,
    pa_insert_old_format_receipt_to_landing,
)

pa_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:pennsylvania"
)


pa_asset_selection = dg.AssetSelection.assets(
    pa_fetch_all_campaign_data_full_export,
    pa_insert_new_format_contrib_to_landing,
    pa_insert_new_format_debt_to_landing,
    pa_insert_new_format_expense_to_landing,
    pa_insert_new_format_filer_to_landing,
    pa_insert_new_format_receipt_to_landing,
    pa_insert_old_format_contrib_to_landing,
    pa_insert_old_format_debt_to_landing,
    pa_insert_old_format_expense_to_landing,
    pa_insert_old_format_filer_to_landing,
    pa_insert_old_format_receipt_to_landing,
)

pa_daily_refresh_selection = pa_asset_selection | pa_dbt_selection


pa_refresh_job = dg.define_asset_job(
    "pa_refresh_job",
    selection=pa_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

pa_daily_refresh_schedule = dg.ScheduleDefinition(
    job=pa_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        pa_fetch_all_campaign_data_full_export,
        pa_insert_new_format_contrib_to_landing,
        pa_insert_old_format_contrib_to_landing,
        pa_insert_new_format_debt_to_landing,
        pa_insert_old_format_debt_to_landing,
        pa_insert_new_format_expense_to_landing,
        pa_insert_old_format_expense_to_landing,
        pa_insert_new_format_filer_to_landing,
        pa_insert_old_format_filer_to_landing,
        pa_insert_new_format_receipt_to_landing,
        pa_insert_old_format_receipt_to_landing,
    ],
    jobs=[pa_refresh_job],
    schedules=[pa_daily_refresh_schedule],
)

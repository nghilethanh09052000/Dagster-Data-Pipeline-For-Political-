import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    wi_expense_transactions_get_final_html,
    wi_expense_transactions_get_intermediate_note,
    wi_expense_transactions_get_post_continue,
    wi_expense_transactions_get_session,
    wi_expense_transactions_insert_to_landing_table,
    wi_filed_reports_get_final_html,
    wi_filed_reports_get_intermediate_note,
    wi_filed_reports_get_post_continue,
    wi_filed_reports_get_session,
    wi_filed_reports_insert_to_landing_table,
    wi_receipt_transactions_get_final_html,
    wi_receipt_transactions_get_intermediate_note,
    wi_receipt_transactions_get_post_continue,
    wi_receipt_transactions_get_session,
    wi_receipt_transactions_insert_to_landing_table,
    wi_registered_committees_get_final_html,
    wi_registered_committees_get_intermediate_note,
    wi_registered_committees_get_post_continue,
    wi_registered_committees_get_session,
    wi_registered_committees_insert_to_landing_table,
)

wi_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:wisconsin"
)

wi_asset_selection = dg.AssetSelection.assets(
    wi_expense_transactions_get_final_html,
    wi_expense_transactions_get_intermediate_note,
    wi_expense_transactions_get_post_continue,
    wi_expense_transactions_get_session,
    wi_expense_transactions_insert_to_landing_table,
    wi_filed_reports_get_final_html,
    wi_filed_reports_get_intermediate_note,
    wi_filed_reports_get_post_continue,
    wi_filed_reports_get_session,
    wi_filed_reports_insert_to_landing_table,
    wi_receipt_transactions_get_final_html,
    wi_receipt_transactions_get_intermediate_note,
    wi_receipt_transactions_get_post_continue,
    wi_receipt_transactions_get_session,
    wi_receipt_transactions_insert_to_landing_table,
    wi_registered_committees_get_final_html,
    wi_registered_committees_get_intermediate_note,
    wi_registered_committees_get_post_continue,
    wi_registered_committees_get_session,
    wi_registered_committees_insert_to_landing_table,
)

wi_daily_refresh_selection = wi_asset_selection | wi_dbt_selection


wi_refresh_job = dg.define_asset_job(
    "wi_refresh_job",
    selection=wi_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

wi_daily_refresh_schedule = dg.ScheduleDefinition(
    job=wi_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        wi_filed_reports_get_session,
        wi_filed_reports_get_intermediate_note,
        wi_filed_reports_get_post_continue,
        wi_filed_reports_get_final_html,
        wi_filed_reports_insert_to_landing_table,
        wi_receipt_transactions_get_session,
        wi_receipt_transactions_get_intermediate_note,
        wi_receipt_transactions_get_post_continue,
        wi_receipt_transactions_get_final_html,
        wi_receipt_transactions_insert_to_landing_table,
        wi_expense_transactions_get_session,
        wi_expense_transactions_get_intermediate_note,
        wi_expense_transactions_get_post_continue,
        wi_expense_transactions_get_final_html,
        wi_expense_transactions_insert_to_landing_table,
        wi_registered_committees_get_session,
        wi_registered_committees_get_intermediate_note,
        wi_registered_committees_get_post_continue,
        wi_registered_committees_get_final_html,
        wi_registered_committees_insert_to_landing_table,
    ],
    jobs=[wi_refresh_job],
    schedules=[wi_daily_refresh_schedule],
)

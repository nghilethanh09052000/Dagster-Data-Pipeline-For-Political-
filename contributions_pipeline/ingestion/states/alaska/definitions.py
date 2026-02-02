import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ak_fetch_all_candidates_initial_page,
    ak_fetch_campaign_disclosure_debt_initial_page,
    ak_fetch_campaign_disclosure_expenditures_initial_page,
    ak_fetch_campaign_disclosure_form_initial_page,
    ak_fetch_campaign_disclosure_income_initial_page,
    ak_fetch_campaign_disclosure_transactions_initial_page,
    ak_fetch_contribution_reports_initial_page,
    ak_inserting_all_candidates_data,
    ak_inserting_campaign_disclosure_debt_data,
    ak_inserting_campaign_disclosure_expenditures_data,
    ak_inserting_campaign_disclosure_form_data,
    ak_inserting_campaign_disclosure_income_data,
    ak_inserting_campaign_disclosure_transactions_data,
    ak_inserting_contribution_reports_data,
    ak_opening_all_candidates_form_export_data,
    ak_opening_campaign_disclosure_debt_form_export_data,
    ak_opening_campaign_disclosure_expenditures_form_export_data,
    ak_opening_campaign_disclosure_form_form_export_data,
    ak_opening_campaign_disclosure_income_export_data,
    ak_opening_contribution_reports_form_export_data,
    ak_opening_disclosure_transations_form_export_data,
    ak_search_all_candidates_table_data,
    ak_search_campaign_disclosure_debt_table_data,
    ak_search_campaign_disclosure_expenditures_table_data,
    ak_search_campaign_disclosure_form_table_data,
    ak_search_campaign_disclosure_income_table_data,
    ak_search_contribution_reports_table_data,
    ak_search_disclosure_transations_table_data,
)

ak_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:alaska"
)

ak_asset_selection = dg.AssetSelection.assets(
    ak_fetch_all_candidates_initial_page,
    ak_fetch_campaign_disclosure_debt_initial_page,
    ak_fetch_campaign_disclosure_expenditures_initial_page,
    ak_fetch_campaign_disclosure_form_initial_page,
    ak_fetch_campaign_disclosure_income_initial_page,
    ak_fetch_campaign_disclosure_transactions_initial_page,
    ak_fetch_contribution_reports_initial_page,
    ak_search_all_candidates_table_data,
    ak_search_campaign_disclosure_debt_table_data,
    ak_search_campaign_disclosure_expenditures_table_data,
    ak_search_campaign_disclosure_form_table_data,
    ak_search_campaign_disclosure_income_table_data,
    ak_search_disclosure_transations_table_data,
    ak_search_contribution_reports_table_data,
    ak_opening_all_candidates_form_export_data,
    ak_opening_campaign_disclosure_debt_form_export_data,
    ak_opening_campaign_disclosure_expenditures_form_export_data,
    ak_opening_campaign_disclosure_form_form_export_data,
    ak_opening_campaign_disclosure_income_export_data,
    ak_opening_disclosure_transations_form_export_data,
    ak_opening_contribution_reports_form_export_data,
    ak_inserting_all_candidates_data,
    ak_inserting_campaign_disclosure_debt_data,
    ak_inserting_campaign_disclosure_expenditures_data,
    ak_inserting_campaign_disclosure_form_data,
    ak_inserting_campaign_disclosure_income_data,
    ak_inserting_campaign_disclosure_transactions_data,
    ak_inserting_contribution_reports_data,
)

ak_daily_refresh_selection = ak_asset_selection | ak_dbt_selection


ak_refresh_job = dg.define_asset_job(
    "ak_refresh_job",
    selection=ak_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=ak_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def ak_daily_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[
        ak_fetch_all_candidates_initial_page,
        ak_fetch_campaign_disclosure_debt_initial_page,
        ak_fetch_campaign_disclosure_expenditures_initial_page,
        ak_fetch_campaign_disclosure_form_initial_page,
        ak_fetch_campaign_disclosure_income_initial_page,
        ak_fetch_campaign_disclosure_transactions_initial_page,
        ak_fetch_contribution_reports_initial_page,
        ak_search_all_candidates_table_data,
        ak_search_campaign_disclosure_debt_table_data,
        ak_search_campaign_disclosure_expenditures_table_data,
        ak_search_campaign_disclosure_form_table_data,
        ak_search_campaign_disclosure_income_table_data,
        ak_search_disclosure_transations_table_data,
        ak_search_contribution_reports_table_data,
        ak_opening_all_candidates_form_export_data,
        ak_opening_campaign_disclosure_debt_form_export_data,
        ak_opening_campaign_disclosure_expenditures_form_export_data,
        ak_opening_campaign_disclosure_form_form_export_data,
        ak_opening_campaign_disclosure_income_export_data,
        ak_opening_disclosure_transations_form_export_data,
        ak_opening_contribution_reports_form_export_data,
        ak_inserting_all_candidates_data,
        ak_inserting_campaign_disclosure_debt_data,
        ak_inserting_campaign_disclosure_expenditures_data,
        ak_inserting_campaign_disclosure_form_data,
        ak_inserting_campaign_disclosure_income_data,
        ak_inserting_campaign_disclosure_transactions_data,
        ak_inserting_contribution_reports_data,
    ],
    jobs=[ak_refresh_job],
    schedules=[ak_daily_refresh_schedule],
)

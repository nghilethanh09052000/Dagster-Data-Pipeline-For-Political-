import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    fetch_virginia_campaign_finance,
    va_candidate_campaign_committee_insert_to_landing_table,
    va_inagural_committee_insert_to_landing_table,
    va_old_schedule_b_insert_to_landing_table,
    va_old_schedule_c_insert_to_landing_table,
    va_old_schedule_d_insert_to_landing_table,
    va_old_schedule_e_insert_to_landing_table,
    va_old_schedule_f_insert_to_landing_table,
    va_old_schedule_g_insert_to_landing_table,
    va_old_schedule_h_insert_to_landing_table,
    va_old_schedule_i_insert_to_landing_table,
    va_out_of_state_political_action_committee_insert_to_landing_table,
    va_political_action_committee_insert_to_landing_table,
    va_political_party_committee_insert_to_landing_table,
    va_referendum_committee_insert_to_landing_table,
    va_report_insert_to_landing_table,
    va_schedule_a_insert_to_landing_table,
    va_schedule_a_pac_insert_to_landing_table,
    va_schedule_b_insert_to_landing_table,
    va_schedule_b_pac_insert_to_landing_table,
    va_schedule_c_insert_to_landing_table,
    va_schedule_c_pac_insert_to_landing_table,
    va_schedule_d_insert_to_landing_table,
    va_schedule_d_pac_insert_to_landing_table,
    va_schedule_e_insert_to_landing_table,
    va_schedule_e_pac_insert_to_landing_table,
    va_schedule_f_insert_to_landing_table,
    va_schedule_f_pac_insert_to_landing_table,
    va_schedule_g_insert_to_landing_table,
    va_schedule_g_pac_insert_to_landing_table,
    va_schedule_h_insert_to_landing_table,
    va_schedule_h_pac_insert_to_landing_table,
    va_schedule_i_insert_to_landing_table,
    va_schedule_i_pac_insert_to_landing_table,
    va_transitional_schedule_a_pac_insert_to_landing_table,
    va_transitional_schedule_d_insert_to_landing_table,
    va_transitional_schedule_d_pac_insert_to_landing_table,
)

va_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:virginia"
)
va_asset_selection = dg.AssetSelection.assets(
    fetch_virginia_campaign_finance,
    va_candidate_campaign_committee_insert_to_landing_table,
    va_inagural_committee_insert_to_landing_table,
    va_old_schedule_b_insert_to_landing_table,
    va_old_schedule_c_insert_to_landing_table,
    va_old_schedule_d_insert_to_landing_table,
    va_old_schedule_e_insert_to_landing_table,
    va_old_schedule_f_insert_to_landing_table,
    va_old_schedule_g_insert_to_landing_table,
    va_old_schedule_h_insert_to_landing_table,
    va_old_schedule_i_insert_to_landing_table,
    va_out_of_state_political_action_committee_insert_to_landing_table,
    va_political_action_committee_insert_to_landing_table,
    va_political_party_committee_insert_to_landing_table,
    va_referendum_committee_insert_to_landing_table,
    va_report_insert_to_landing_table,
    va_schedule_a_insert_to_landing_table,
    va_schedule_a_pac_insert_to_landing_table,
    va_schedule_b_insert_to_landing_table,
    va_schedule_b_pac_insert_to_landing_table,
    va_schedule_c_insert_to_landing_table,
    va_schedule_c_pac_insert_to_landing_table,
    va_schedule_d_insert_to_landing_table,
    va_schedule_d_pac_insert_to_landing_table,
    va_schedule_e_insert_to_landing_table,
    va_schedule_e_pac_insert_to_landing_table,
    va_schedule_f_insert_to_landing_table,
    va_schedule_f_pac_insert_to_landing_table,
    va_schedule_g_insert_to_landing_table,
    va_schedule_g_pac_insert_to_landing_table,
    va_schedule_h_insert_to_landing_table,
    va_schedule_h_pac_insert_to_landing_table,
    va_schedule_i_insert_to_landing_table,
    va_schedule_i_pac_insert_to_landing_table,
    va_transitional_schedule_a_pac_insert_to_landing_table,
    va_transitional_schedule_d_insert_to_landing_table,
    va_transitional_schedule_d_pac_insert_to_landing_table,
)

# Combine both selection
va_daily_refresh_selection = va_asset_selection | va_dbt_selection

va_refresh_job = dg.define_asset_job(
    "va_refresh_job",
    selection=va_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

va_daily_refresh_schedule = dg.ScheduleDefinition(
    job=va_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


defs = dg.Definitions(
    assets=[
        fetch_virginia_campaign_finance,
        va_candidate_campaign_committee_insert_to_landing_table,
        va_inagural_committee_insert_to_landing_table,
        va_old_schedule_b_insert_to_landing_table,
        va_old_schedule_c_insert_to_landing_table,
        va_old_schedule_d_insert_to_landing_table,
        va_old_schedule_e_insert_to_landing_table,
        va_old_schedule_f_insert_to_landing_table,
        va_old_schedule_g_insert_to_landing_table,
        va_old_schedule_h_insert_to_landing_table,
        va_old_schedule_i_insert_to_landing_table,
        va_out_of_state_political_action_committee_insert_to_landing_table,
        va_political_action_committee_insert_to_landing_table,
        va_political_party_committee_insert_to_landing_table,
        va_referendum_committee_insert_to_landing_table,
        va_report_insert_to_landing_table,
        va_schedule_a_insert_to_landing_table,
        va_schedule_a_pac_insert_to_landing_table,
        va_schedule_b_insert_to_landing_table,
        va_schedule_b_pac_insert_to_landing_table,
        va_schedule_c_insert_to_landing_table,
        va_schedule_c_pac_insert_to_landing_table,
        va_schedule_d_insert_to_landing_table,
        va_schedule_d_pac_insert_to_landing_table,
        va_schedule_e_insert_to_landing_table,
        va_schedule_e_pac_insert_to_landing_table,
        va_schedule_f_insert_to_landing_table,
        va_schedule_f_pac_insert_to_landing_table,
        va_schedule_g_insert_to_landing_table,
        va_schedule_g_pac_insert_to_landing_table,
        va_schedule_h_insert_to_landing_table,
        va_schedule_h_pac_insert_to_landing_table,
        va_schedule_i_insert_to_landing_table,
        va_schedule_i_pac_insert_to_landing_table,
        va_transitional_schedule_a_pac_insert_to_landing_table,
        va_transitional_schedule_d_insert_to_landing_table,
        va_transitional_schedule_d_pac_insert_to_landing_table,
    ],
    jobs=[va_refresh_job],
    schedules=[va_daily_refresh_schedule],
)

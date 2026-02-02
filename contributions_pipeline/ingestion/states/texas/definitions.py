import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    tx_assets_insert_to_landing_table,
    tx_cand_insert_to_landing_table,
    tx_cont_ss_insert_to_landing_table,
    tx_cont_t_insert_to_landing_table,
    tx_contribs_insert_to_landing_table,
    tx_cover_insert_to_landing_table,
    tx_credits_insert_to_landing_table,
    tx_debts_insert_to_landing_table,
    tx_expend_insert_to_landing_table,
    tx_expn_catg_insert_to_landing_table,
    tx_expn_t_insert_to_landing_table,
    tx_fetch_zip_data,
    tx_filers_insert_to_landing_table,
    tx_finals_insert_to_landing_table,
    tx_loans_insert_to_landing_table,
    tx_notices_insert_to_landing_table,
    tx_pledges_insert_to_landing_table,
    tx_purpose_insert_to_landing_table,
    tx_spacs_insert_to_landing_table,
    tx_travel_insert_to_landing_table,
)

tx_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:texas"
)
tx_asset_selection = dg.AssetSelection.assets(
    tx_assets_insert_to_landing_table,
    tx_cand_insert_to_landing_table,
    tx_cont_ss_insert_to_landing_table,
    tx_cont_t_insert_to_landing_table,
    tx_contribs_insert_to_landing_table,
    tx_cover_insert_to_landing_table,
    tx_credits_insert_to_landing_table,
    tx_debts_insert_to_landing_table,
    tx_expend_insert_to_landing_table,
    tx_expn_catg_insert_to_landing_table,
    tx_expn_t_insert_to_landing_table,
    tx_fetch_zip_data,
    tx_filers_insert_to_landing_table,
    tx_finals_insert_to_landing_table,
    tx_loans_insert_to_landing_table,
    tx_notices_insert_to_landing_table,
    tx_pledges_insert_to_landing_table,
    tx_purpose_insert_to_landing_table,
    tx_spacs_insert_to_landing_table,
    tx_travel_insert_to_landing_table,
)

# Combine both selection
tx_daily_refresh_selection = tx_asset_selection | tx_dbt_selection


tx_refresh_job = dg.define_asset_job(
    "tx_refresh_job",
    selection=tx_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

tx_daily_refresh_schedule = dg.ScheduleDefinition(
    job=tx_refresh_job,
    cron_schedule=r"0 17 * * 0",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        tx_fetch_zip_data,
        tx_assets_insert_to_landing_table,
        tx_cand_insert_to_landing_table,
        tx_cont_ss_insert_to_landing_table,
        tx_cont_t_insert_to_landing_table,
        tx_contribs_insert_to_landing_table,
        tx_cover_insert_to_landing_table,
        tx_credits_insert_to_landing_table,
        tx_debts_insert_to_landing_table,
        tx_expend_insert_to_landing_table,
        tx_expn_catg_insert_to_landing_table,
        tx_expn_t_insert_to_landing_table,
        tx_filers_insert_to_landing_table,
        tx_finals_insert_to_landing_table,
        tx_loans_insert_to_landing_table,
        tx_notices_insert_to_landing_table,
        tx_pledges_insert_to_landing_table,
        tx_purpose_insert_to_landing_table,
        tx_spacs_insert_to_landing_table,
        tx_travel_insert_to_landing_table,
    ],
    jobs=[tx_refresh_job],
    schedules=[tx_daily_refresh_schedule],
)

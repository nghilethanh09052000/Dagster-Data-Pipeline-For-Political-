import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    nv_download_campaign_finance_data,
    nv_insert_candidates_to_landing_table,
    nv_insert_contribuions_to_landing_table,
    nv_insert_contributors_payees_to_landing_table,
    nv_insert_expenses_to_landing_table,
    nv_insert_groups_to_landing_table,
    nv_insert_reports_to_landing_table,
)

nv_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:nevada"
)
nv_asset_selection = dg.AssetSelection.assets(
    nv_download_campaign_finance_data,
    nv_insert_candidates_to_landing_table,
    nv_insert_groups_to_landing_table,
    nv_insert_reports_to_landing_table,
    nv_insert_contributors_payees_to_landing_table,
    nv_insert_contribuions_to_landing_table,
    nv_insert_expenses_to_landing_table,
)

# Combine both selection
nv_daily_refresh_selection = nv_asset_selection | nv_dbt_selection


nv_refresh_job = dg.define_asset_job(
    "nv_refresh_job",
    selection=nv_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

defs = dg.Definitions(
    assets=[
        nv_download_campaign_finance_data,
        nv_insert_candidates_to_landing_table,
        nv_insert_groups_to_landing_table,
        nv_insert_reports_to_landing_table,
        nv_insert_contributors_payees_to_landing_table,
        nv_insert_contribuions_to_landing_table,
        nv_insert_expenses_to_landing_table,
    ],
    jobs=[nv_refresh_job],
)

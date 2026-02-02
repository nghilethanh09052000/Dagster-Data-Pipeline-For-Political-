import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ny_all_time_county_candidate,
    ny_all_time_county_committee,
    ny_all_time_state_candidate,
    ny_all_time_state_committee,
    ny_filer_data,
    ny_insert_all_time_county_candidate_to_landing_table,
    ny_insert_all_time_county_committee_to_landing_table,
    ny_insert_all_time_state_candidate_to_landing_table,
    ny_insert_all_time_state_committee_to_landing_table,
    ny_insert_filer_data_to_landing_table,
)

ny_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:new_york"
)
ny_asset_selection = dg.AssetSelection.assets(
    ny_all_time_county_candidate,
    ny_all_time_county_committee,
    ny_all_time_state_candidate,
    ny_all_time_state_committee,
    ny_filer_data,
    ny_insert_all_time_county_candidate_to_landing_table,
    ny_insert_all_time_county_committee_to_landing_table,
    ny_insert_all_time_state_candidate_to_landing_table,
    ny_insert_all_time_state_committee_to_landing_table,
    ny_insert_filer_data_to_landing_table,
)

# Combine both selection
ny_daily_refresh_selection = ny_asset_selection | ny_dbt_selection


ny_refresh_job = dg.define_asset_job(
    "ny_refresh_job",
    selection=ny_daily_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)

defs = dg.Definitions(
    assets=[
        ny_all_time_county_candidate,
        ny_insert_all_time_county_candidate_to_landing_table,
        ny_all_time_county_committee,
        ny_insert_all_time_county_committee_to_landing_table,
        ny_all_time_state_candidate,
        ny_insert_all_time_state_candidate_to_landing_table,
        ny_all_time_state_committee,
        ny_insert_all_time_state_committee_to_landing_table,
        ny_filer_data,
        ny_insert_filer_data_to_landing_table,
    ],
    jobs=[ny_refresh_job],
)

import dagster as dg
from dagster import Definitions
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    download_all_contributions,
    download_all_expenditures,
    insert_all_expenditures,
    insert_contributions,
)

# Create dbt asset selection for Rhode Island
ri_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:rhode_island"
)

# Create selection for existing assets
ri_base_selection = dg.AssetSelection.assets(
    download_all_expenditures,
    insert_all_expenditures,
    download_all_contributions,
    insert_contributions,
)

# Combine both selections
ri_combined_selection = ri_base_selection | ri_dbt_selection

# Define job with combined selection
ri_refresh_job = dg.define_asset_job(
    name="ri_refresh_job",
    selection=ri_combined_selection,
    tags={
        "dagster/retry_strategy": "ALL_STEPS",
        "team": "rhode_island",
    },
)

defs = Definitions(
    assets=[
        download_all_expenditures,
        insert_all_expenditures,
        download_all_contributions,
        insert_contributions,
        dbt_transformations,
    ],
    jobs=[ri_refresh_job],
)

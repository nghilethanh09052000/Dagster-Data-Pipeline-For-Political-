import dagster as dg
from dagster import Definitions
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    oh_download_all_data,
    oh_insert_candidate_contributions,
    oh_insert_candidate_covers,
    oh_insert_candidate_expenditures,
    oh_insert_candidate_lists,
    oh_insert_committee_contributions,
    oh_insert_committee_covers,
    oh_insert_committee_expenditures,
    oh_insert_committee_lists,
    oh_insert_party_contributions,
    oh_insert_party_covers,
    oh_insert_party_expenditures,
)

# Create dbt asset selection for Ohio
oh_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:ohio"
)

# Create selection for Ohio base assets using prefix
oh_base_selection = dg.AssetSelection.keys(
    "oh_download_all_data",
    "oh_insert_candidate_contributions",
    "oh_insert_candidate_expenditures",
    "oh_insert_candidate_lists",
    "oh_insert_candidate_covers",
    "oh_insert_committee_contributions",
    "oh_insert_committee_expenditures",
    "oh_insert_committee_lists",
    "oh_insert_committee_covers",
    "oh_insert_party_contributions",
    "oh_insert_party_expenditures",
    "oh_insert_party_covers",
)

# Combine both selections
oh_combined_selection = oh_base_selection | oh_dbt_selection

# Define job with combined selection
oh_refresh_job = dg.define_asset_job(
    name="oh_refresh_job",
    selection=oh_combined_selection,
    tags={
        "dagster/retry_strategy": "ALL_STEPS",
        "team": "ohio",
    },
)

defs = Definitions(
    assets=[
        # Ohio-specific assets
        oh_download_all_data,
        oh_insert_candidate_contributions,
        oh_insert_candidate_expenditures,
        oh_insert_candidate_lists,
        oh_insert_candidate_covers,
        oh_insert_committee_contributions,
        oh_insert_committee_expenditures,
        oh_insert_committee_lists,
        oh_insert_committee_covers,
        oh_insert_party_contributions,
        oh_insert_party_expenditures,
        oh_insert_party_covers,
    ],
    jobs=[oh_refresh_job],
)

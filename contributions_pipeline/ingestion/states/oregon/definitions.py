import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    or_fetch_committees_data,
    or_fetch_contributions_data,
    or_insert_commitees_to_landing,
    or_insert_contributions_to_landing,
)

or_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:oregon"
)
or_asset_selection = dg.AssetSelection.assets(
    or_fetch_contributions_data,
    or_insert_contributions_to_landing,
    or_fetch_committees_data,
    or_insert_commitees_to_landing,
)

# Combine both selection
or_daily_refresh_selection = or_asset_selection | or_dbt_selection

or_refresh_job = dg.define_asset_job(
    "or_refresh_job",
    selection=or_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

defs = dg.Definitions(
    assets=[
        or_fetch_contributions_data,
        or_insert_contributions_to_landing,
        or_fetch_committees_data,
        or_insert_commitees_to_landing,
    ],
    jobs=[or_refresh_job],
)

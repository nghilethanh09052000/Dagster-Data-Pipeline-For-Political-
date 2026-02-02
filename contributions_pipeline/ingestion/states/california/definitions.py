import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    ca_cvr2_campaign_disclosure_insert_to_landing_table,
    ca_cvr2_so_insert_to_landing_table,
    ca_cvr3_verification_info_insert_to_landing_table,
    ca_cvr_campaign_disclosure_insert_to_landing_table,
    ca_cvr_e530_insert_to_landing_table,
    ca_cvr_f470_insert_to_landing_table,
    ca_cvr_so_insert_to_landing_table,
    ca_debt_insert_to_landing_table,
    ca_expn_insert_to_landing_table,
    ca_f495p2_insert_to_landing_table,
    ca_f501_502_insert_to_landing_table,
    ca_fetch_cal_access_raw_data,
    ca_loan_insert_to_landing_table,
    ca_rcpt_insert_to_landing_table,
    ca_s401_insert_to_landing_table,
    ca_s496_insert_to_landing_table,
    ca_s497_insert_to_landing_table,
    ca_s498_insert_to_landing_table,
)

ca_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:california"
)
ca_asset_selection = dg.AssetSelection.assets(
    ca_fetch_cal_access_raw_data,
    ca_cvr_e530_insert_to_landing_table,
    ca_f501_502_insert_to_landing_table,
    ca_cvr3_verification_info_insert_to_landing_table,
    ca_cvr_so_insert_to_landing_table,
    ca_cvr2_so_insert_to_landing_table,
    ca_cvr_campaign_disclosure_insert_to_landing_table,
    ca_cvr2_campaign_disclosure_insert_to_landing_table,
    ca_cvr_f470_insert_to_landing_table,
    ca_s401_insert_to_landing_table,
    ca_f495p2_insert_to_landing_table,
    ca_s496_insert_to_landing_table,
    ca_s497_insert_to_landing_table,
    ca_s498_insert_to_landing_table,
    ca_debt_insert_to_landing_table,
    ca_loan_insert_to_landing_table,
    ca_expn_insert_to_landing_table,
    ca_rcpt_insert_to_landing_table,
)

ca_daily_refresh_selection = ca_asset_selection | ca_dbt_selection

ca_refresh_job = dg.define_asset_job(
    "ca_refresh_job",
    selection=ca_daily_refresh_selection,
    tags={
        # On Dagster+ as it is "Serverless", all file system are temporary
        # thus on retry we should re-fetch all the files
        # Ref: https://docs.dagster.io/guides/deploy/execution/run-retries#retry-strategy
        "dagster/retry_strategy": "ALL_STEPS"
    },
)

ca_daily_refresh_schedule = dg.ScheduleDefinition(
    job=ca_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


defs = dg.Definitions(
    assets=[
        ca_fetch_cal_access_raw_data,
        ca_cvr_e530_insert_to_landing_table,
        ca_f501_502_insert_to_landing_table,
        ca_cvr3_verification_info_insert_to_landing_table,
        ca_cvr_so_insert_to_landing_table,
        ca_cvr2_so_insert_to_landing_table,
        ca_cvr_campaign_disclosure_insert_to_landing_table,
        ca_cvr2_campaign_disclosure_insert_to_landing_table,
        ca_cvr_f470_insert_to_landing_table,
        ca_s401_insert_to_landing_table,
        ca_f495p2_insert_to_landing_table,
        ca_s496_insert_to_landing_table,
        ca_s497_insert_to_landing_table,
        ca_s498_insert_to_landing_table,
        ca_debt_insert_to_landing_table,
        ca_loan_insert_to_landing_table,
        ca_expn_insert_to_landing_table,
        ca_rcpt_insert_to_landing_table,
    ],
    jobs=[ca_refresh_job],
    schedules=[ca_daily_refresh_schedule],
)

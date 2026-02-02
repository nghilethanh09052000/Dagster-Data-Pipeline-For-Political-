import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    wv_fetch_candidates_raw_data,
    wv_fetch_committees_raw_data,
    wv_fetch_contributions_raw_data,
    wv_fetch_expenditures_raw_data,
    wv_insert_candidates_to_landing_table,
    wv_insert_committees_to_landing_table,
    wv_insert_raw_contributions_to_landing_table,
    wv_insert_raw_expenditures_to_landing_table,
)

wv_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:west_virginia"
)

wv_asset_selection = dg.AssetSelection.assets(
    wv_fetch_candidates_raw_data,
    wv_fetch_contributions_raw_data,
    wv_insert_raw_contributions_to_landing_table,
    wv_fetch_committees_raw_data,
    wv_insert_candidates_to_landing_table,
    wv_insert_committees_to_landing_table,
    wv_fetch_expenditures_raw_data,
    wv_insert_raw_expenditures_to_landing_table,
)

wv_yearly_refresh_selection = wv_asset_selection | wv_dbt_selection

wv_yearly_refresh_job = dg.define_asset_job(
    "wv_yearly_refresh_job",
    selection=wv_yearly_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=wv_yearly_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def wv_yearly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[
        wv_fetch_candidates_raw_data,
        wv_insert_candidates_to_landing_table,
        wv_fetch_contributions_raw_data,
        wv_insert_raw_contributions_to_landing_table,
        wv_fetch_committees_raw_data,
        wv_insert_committees_to_landing_table,
        wv_fetch_expenditures_raw_data,
        wv_insert_raw_expenditures_to_landing_table,
    ],
    jobs=[wv_yearly_refresh_job],
    schedules=[wv_yearly_refresh_schedule],
)

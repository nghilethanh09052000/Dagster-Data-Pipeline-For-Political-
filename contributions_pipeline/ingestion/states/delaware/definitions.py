import dagster as dg
from dagster_dbt import build_dbt_asset_selection

from contributions_pipeline.transformation.assets import dbt_transformations

from .assets import (
    de_fetch_candidates_data,
    de_fetch_committees_data,
    de_fetch_contributions_data,
    de_fetch_expenditures_data,
    de_fetch_filed_reports_data,
    de_insert_candidates_to_landing_table,
    de_insert_committees_to_landing_table,
    de_insert_contributions_to_landing_table,
    de_insert_expenditures_to_landing_table,
    de_insert_filed_reports_to_landing_table,
)

de_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:delaware"
)

de_asset_selection = dg.AssetSelection.assets(
    de_fetch_candidates_data,
    de_fetch_contributions_data,
    de_insert_contributions_to_landing_table,
    de_fetch_committees_data,
    de_insert_candidates_to_landing_table,
    de_insert_committees_to_landing_table,
    de_fetch_filed_reports_data,
    de_insert_filed_reports_to_landing_table,
    de_fetch_expenditures_data,
    de_insert_expenditures_to_landing_table,
)

de_yearly_refresh_selection = de_asset_selection | de_dbt_selection

de_yearly_refresh_job = dg.define_asset_job(
    "de_yearly_refresh_job",
    selection=de_yearly_refresh_selection,
    tags={"dagster/retry_strategy": "ALL_STEPS"},
)


@dg.schedule(
    job=de_yearly_refresh_job,
    cron_schedule=r"0 17 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def de_yearly_refresh_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(partition_key=context.scheduled_execution_time.strftime("%Y"))


defs = dg.Definitions(
    assets=[
        de_fetch_candidates_data,
        de_insert_candidates_to_landing_table,
        de_fetch_contributions_data,
        de_insert_contributions_to_landing_table,
        de_fetch_committees_data,
        de_insert_committees_to_landing_table,
        de_fetch_filed_reports_data,
        de_insert_filed_reports_to_landing_table,
        de_fetch_expenditures_data,
        de_insert_expenditures_to_landing_table,
    ],
    jobs=[de_yearly_refresh_job],
    schedules=[de_yearly_refresh_schedule],
)

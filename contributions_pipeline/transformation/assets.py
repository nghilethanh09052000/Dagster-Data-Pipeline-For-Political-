import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

from .resource import dbt_transformations_project


@dbt_assets(manifest=dbt_transformations_project.manifest_path)
def dbt_transformations(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """
    Run transformations from dbt project `dbt_transformations`
    """
    yield from dbt.cli(["build"], context=context).stream()

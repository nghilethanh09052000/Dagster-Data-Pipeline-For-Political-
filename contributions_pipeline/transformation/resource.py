import os
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject


def get_profile_based_on_env():
    """
    Get the corrent profile based on the current environment
    """
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        # CHANGEME if we have separate branch db
        return "prod"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod":
        return "prod"
    return "dev"


CURRENT_FILE_DIR = Path(__file__).absolute().parent
DBT_PROJECT_ROOT_DIR = CURRENT_FILE_DIR / "dbt_transformations"

dbt_transformations_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROJECT_ROOT_DIR,
    target=get_profile_based_on_env(),
)
dbt_transformations_project.prepare_if_dev()

dbt_cli_resource = DbtCliResource(project_dir=dbt_transformations_project)

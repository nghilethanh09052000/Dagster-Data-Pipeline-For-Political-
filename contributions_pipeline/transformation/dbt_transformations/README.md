# DBT Transformations

This DBT project contains all the DBT transformations used to get data from landing table to something more useful for our use cases.

## Usage

For most part, you will be running all DBT transformations through Dagster. Check [DBT Core CLI docs](https://docs.getdbt.com/reference/dbt-commands) if you require other DBT related actions.

## Dagster Integrations

To make sure that all of your DBT transformations have the correct dependencies on Dagster do the following.

1. Add your source table to [`sources.yml`](./models/sources.yml), follow the table list format already in there, and add the correct `asset_key` on Dagster Meta to make your transformations later have the right asset dependencies.
2. Create your SQL file on [`models`](./models/) that uses your newly added source (`{{ source('...', '...') }}`).
3. Reload your Dagster, and now your new SQL file should show up as an asset that depends on the other asset you've added to the `sources.yml`.

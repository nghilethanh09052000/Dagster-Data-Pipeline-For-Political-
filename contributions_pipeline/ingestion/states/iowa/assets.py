import csv
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

IA_DATA_PATH_PREFIX = "./states/iowa"

IA_DATASETS = [
    {
        "name": "campaign_contributions_received",
        "api_url": (
            "https://data.iowa.gov/api/views/smfg-ds7h/rows.csv"
            "?fourfour=smfg-ds7h"
            "&cacheBust=1743487303"
            "&date={date}"
            "&accessType=DOWNLOAD"
        ),
        "columns": [
            "date",
            "committee_code",
            "committee_type",
            "committee_name",
            "transaction_type",
            "contributing_committee_code",
            "contributing_organization",
            "first_name",
            "last_name",
            "address_line_1",
            "address_line_2",
            "city",
            "state",
            "zip_code",
            "contribution_amount",
            "check_number",
        ],
    },
    {
        "name": "campaign_expenditures",
        "api_url": (
            "https://data.iowa.gov/api/views/3adi-mht4/rows.csv"
            "?fourfour=3adi-mht4"
            "&cacheBust=1743483932"
            "&date={date}"
            "&accessType=DOWNLOAD"
        ),
        "columns": [
            "date",
            "transaction_type",
            "committee_code",
            "committee_type",
            "committee_name",
            "receiving_committee_code",
            "receiving_organization_name",
            "first_name",
            "last_name",
            "address_line_1",
            "address_line_2",
            "city",
            "state",
            "zip",
            "expenditure_amount",
            "check_number",
        ],
    },
    {
        "name": "independent_expenditures",
        "api_url": (
            "https://data.iowa.gov/api/views/8u5j-u74n/rows.csv"
            "?fourfour=8u5j-u74n"
            "&cacheBust=1743483795"
            "&date={date}"
            "&accessType=DOWNLOAD"
        ),
        "columns": [
            "organization_name",
            "organization_city",
            "organization_state",
            "organization_zip",
            "organization_email",
            "contact_first_name",
            "contact_last_name",
            "contact_email",
            "date_of_expenditure",
            "amount",
            "position",
            "communication_description",
            "communication_type",
            "corporation",
            "vendor",
            "distribution_date",
            "for_candidate",
            "against_candidate",
            "for_ballot_issue",
            "against_ballot_issue",
            "signed_date",
        ],
    },
    {
        "name": "registered_political_candidates",
        "api_url": (
            "https://data.iowa.gov/api/views/5dtu-swbk/rows.csv"
            "?fourfour=5dtu-swbk"
            "&cacheBust=1743483659"
            "&date={date}"
            "&accessType=DOWNLOAD"
        ),
        "columns": [
            "committee_name",
            "committee_type",
            "committee_number",
            "district",
            "party",
            "election_year",
            "election_date",
            "office_sought",
            "county",
            "candidate_name",
            "candidate_address",
            "candidate_city_state_zip",
            "candidate_phone",
            "candidate_email",
            "chair_name",
            "chair_address",
            "chair_city_state_zip",
            "chair_phone",
            "chair_email",
            "treasurer_name",
            "treasurer_address",
            "treasurer_city_state_zip",
            "treasurer_phone",
            "treasurer_email",
            "parent_entity",
            "parent_address",
            "parent_city_state_zip",
            "contact_name",
            "contact_address",
            "contact_city_state_zip",
            "contact_phone",
        ],
    },
]


def ia_insert_data_to_landing_table(
    postgres_pool: ConnectionPool,
    dataset_name: str,
    table_name: str,
    columns: list[str],
    data_validation_callback: Callable,
    data_file: str,
):
    """
    Insert raw ia-ACCESS data to landing table in Postgres.

    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource pnhams.
                     You can add `pg: dg.ResourcePnham[PostgresResource]` to your asset
                     pnhameters to get accees to the resource.

    dataset_name: the DATASETS mapping we use to check to get the correct file path

    table_name: the name of the table that's going to be used for ingesting

    columns: list of columns name in order of the data files that's going to
                        be inserted.

    data_validation_callback: callable to validate the cleaned rows, if the function
                            return is False, the row will be ignored, a wnhning
                            will be shown on the logs

    data_file: the file path of the actual data file on the Iowa data folder,
                        so that we will have correct path and use
                        it to ingest to table
    """

    logger = dg.get_dagster_logger(name=f"ia_{dataset_name}_insert")
    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name} before inserting new data.")
        pg_cursor.execute(truncate_query)

        logger.info(f"Processing file: {data_file}")
        data_type_lines_generator = safe_readline_csv_like_file(
            data_file, encoding="utf-8"
        )

        # Read as comma-separated values
        parsed_data_type_file = csv.reader(data_type_lines_generator, delimiter=",")

        # Skip the header
        next(parsed_data_type_file, None)

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_data_type_file,
            table_name=table_name,
            table_columns_name=columns,
            row_validation_callback=data_validation_callback,
        )

        # Verify row count
        count_query = get_sql_count_query(table_name=table_name)

        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Successfully inserted {row_count} rows into {table_name}")
        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


def ia_fetch_raw_data(dataset_name: str, api_url: str):
    """
    Fetches and saves raw CSV data from the given Iowa campaign finance API URL.

    This function creates the destination directory if it doesn't exist, downloads
    the file via streaming to handle large files efficiently, and saves it to a
    local CSV file named after the dataset.

    Args:
        dataset_name (str): The name of the dataset to use as the output file name.
        api_url (str): The full API URL to download the dataset from.

    Returns:
        None

    Side Effects:
        - Creates the output directory if it doesn't exist.
        - Saves the downloaded file to disk as a CSV.
        - Logs the location of the saved dataset.
    """

    logger = dg.get_dagster_logger(name=f"ia_{dataset_name}_fetch")

    Path(IA_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)
    csv_filename = f"{IA_DATA_PATH_PREFIX}/{dataset_name}.csv"

    stream_download_file_to_path(api_url, csv_filename)

    logger.info(f"Dataset {dataset_name} saved to {csv_filename}")


def ia_create_fetch_raw_data_assets(dataset: dict):
    @dg.asset(
        name=f"ia_{dataset.get('name')}_fetch_raw_data",
    )
    def fetch_data():
        formatted_date = datetime.today().strftime("%Y%m%d")
        api_url = dataset["api_url"].format(date=formatted_date)
        return ia_fetch_raw_data(dataset_name=dataset["name"], api_url=api_url)

    return fetch_data


def ia_create_insert_data_to_landing_table_assets(dataset):
    fetch_asset_name = f"ia_{dataset.get('name')}_fetch_raw_data"

    @dg.asset(
        deps=[fetch_asset_name],
        name=f"ia_{dataset.get('name')}_insert_data_to_landing_table",
    )
    def insert_data(pg: dg.ResourceParam[PostgresResource]):
        dataset_name = dataset["name"]
        table_name = f"ia_{dataset['name']}_landing"
        columns = dataset["columns"]
        data_file = f"{IA_DATA_PATH_PREFIX}/{dataset_name}.csv"

        return ia_insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name=dataset["name"],
            table_name=table_name,
            columns=columns,
            data_validation_callback=lambda row: len(row) == len(columns),
            data_file=data_file,
        )

    return insert_data


fetch_assets = [ia_create_fetch_raw_data_assets(dataset) for dataset in IA_DATASETS]

insert_assets = [
    ia_create_insert_data_to_landing_table_assets(dataset) for dataset in IA_DATASETS
]

assets = fetch_assets + insert_assets

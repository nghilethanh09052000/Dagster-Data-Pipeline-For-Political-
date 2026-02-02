import csv
from collections.abc import Callable
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

BASE_URL = "https://elections.il.gov/campaigndisclosuredatafiles/"
DATA_PATH_PREFIX = "./states/illinois"

DATASETS = [
    {
        "name": "Candidates",
        "table_name": "il_candidates_landing",
        "columns": [
            "ID",
            "LastName",
            "FirstName",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Office",
            "DistrictType",
            "District",
            "ResidenceCounty",
            "PartyAffiliation",
            "RedactionRequested",
        ],
    },
    {
        "name": "CmteCandidateLinks",
        "table_name": "il_cmte_candidate_links_landing",
        "columns": ["ID", "CommitteeID", "CandidateID"],
    },
    {
        "name": "CmteOfficerLinks",
        "table_name": "il_cte_officer_links_landing",
        "columns": ["ID", "CommitteeID", "OfficerID"],
    },
    {
        "name": "Committees",
        "table_name": "il_committees_landing",
        "columns": [
            "ID",
            "TypeOfCommittee",
            "StateCommittee",
            "StateID",
            "LocalCommittee",
            "LocalID",
            "ReferName",
            "Name",
            "Address1",
            "Address2",
            "Address3",
            "City",
            "State",
            "Zip",
            "Status",
            "StatusDate",
            "CreationDate",
            "CreationAmount",
            "DispFundsReturn",
            "DispFundsPolComm",
            "DispFundsCharity",
            "DispFunds95",
            "DispFundsDescrip",
            "CanSuppOpp",
            "PolicySuppOpp",
            "PartyAffiliation",
            "Purpose",
        ],
    },
    {
        "name": "D2Totals",
        "table_name": "il_d2_totals_landing",
        "columns": [
            "ID",
            "CommitteeID",
            "FiledDocID",
            "BegFundsAvail",
            "IndivContribI",
            "IndivContribNI",
            "XferInI",
            "XferInNI",
            "LoanRcvI",
            "LoanRcvNI",
            "OtherRctI",
            "OtherRctNI",
            "TotalReceipts",
            "InKindI",
            "InKindNI",
            "TotalInKind",
            "XferOutI",
            "XferOutNI",
            "LoanMadeI",
            "LoanMadeNI",
            "ExpendI",
            "ExpendNI",
            "IndependentExpI",
            "IndependentExpNI",
            "TotalExpend",
            "DebtsI",
            "DebtsNI",
            "TotalDebts",
            "TotalInvest",
            "EndFundsAvail",
            "Archived",
        ],
    },
    {
        "name": "Expenditures",
        "table_name": "il_expenditures_landing",
        "columns": [
            "ID",
            "CommitteeID",
            "FiledDocID",
            "ETransID",
            "LastOnlyName",
            "FirstName",
            "ExpendedDate",
            "Amount",
            "AggregateAmount",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "D2Part",
            "Purpose",
            "CandidateName",
            "Office",
            "Supporting",
            "Opposing",
            "Archived",
            "Country",
            "RedactionRequested",
        ],
    },
    {
        "name": "Investments",
        "table_name": "il_investments_landing",
        "columns": [
            "ID",
            "CommitteeID",
            "FiledDocID",
            "Description",
            "PurchaseDate",
            "PurchaseShares",
            "PurchasePrice",
            "CurrentValue",
            "LiquidValue",
            "LiquidDate",
            "LastOnlyName",
            "FirstName",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Archived",
            "Country",
        ],
    },
    {
        "name": "Officers",
        "table_name": "il_officers_landing",
        "columns": [
            "ID",
            "LastName",
            "FirstName",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Title",
            "Phone",
            "RedactionRequested",
        ],
    },
    {
        "name": "Receipts",
        "table_name": "il_receipts_landing",
        "columns": [
            "ID",
            "CommitteeID",
            "FiledDocID",
            "ETransID",
            "LastOnlyName",
            "FirstName",
            "RcvDate",
            "Amount",
            "AggregateAmount",
            "LoanAmount",
            "Occupation",
            "Employer",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "D2Part",
            "Description",
            "VendorLastOnlyName",
            "VendorFirstName",
            "VendorAddress1",
            "VendorAddress2",
            "VendorCity",
            "VendorState",
            "VendorZip",
            "Archived",
            "Country",
            "RedactionRequested",
        ],
    },
]


def il_fetch_bulk_data(
    file_name: str,
):
    """
    Fetches bulk data from the given API URL and saves it as a CSV file.

    Parameters:
    - file_name (str): The name of the file to be downloaded.

    Returns:
    - None: Saves the fetched data as a CSV file in the designated directory.
    """
    logger = dg.get_dagster_logger(name="il_fetch_bulk_data")
    logger.info(f"IL Starting Fetch Bulk Data for {file_name}")

    dataset_dir = Path(DATA_PATH_PREFIX)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = dataset_dir / f"{file_name}.csv"

    url = f"{BASE_URL}{file_name}.txt"
    stream_download_file_to_path(url, csv_file_path)
    logger.info(f"Saved csv file to {csv_file_path}")


def insert_data_to_landing_table(
    postgres_pool: ConnectionPool,
    dataset_name: str,
    table_name: str,
    columns: list[str],
    data_validation_callback: Callable,
    data_file: str,
):
    """
    Insert raw data to landing table in Postgres.

    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.

    dataset_name: the DATASETS mapping we use to check to get the correct file path

    table_name: the name of the table that's going to be used for ingesting

    columns: list of columns name in order of the data files that's going to
                        be inserted.

    data_validation_callback: callable to validate the cleaned rows, if the function
                            return is false, the row will be ignored, a warning
                            will be shown on the logs

    data_file: the file path of the actual data file on the illinois folder,
    """

    logger = dg.get_dagster_logger(name=f"il_{dataset_name}_insert")
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

        # Read as tab-separated values
        parsed_data_type_file = csv.reader(data_type_lines_generator, delimiter="\t")

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


def il_create_fetch_raw_data_assets(filename: str):
    """
    Creates a Dagster asset that fetches raw data from the
    specified API and processes it into a structured format.

    This asset is responsible for fetching data
    from API based on the provided dataset configuration.

    The asset fetches the raw data using the dataset's
    API URL and columns, and it is typically followed by an insert
    operation into a landing table.

    Args:
        'filename' (str): The name of the dataset.

    Returns:
        function: A Dagster asset function that fetches raw data from the API.
        This function will fetch and process the data when executed.

    The fetch asset performs the following:
        - It fetches the raw data from the API specified by `dataset["url"]`.
        - It processes the data based on the provided columns,
            structuring the raw data into a usable format.

    Example:
        il_create_fetch_raw_data_assets({
            "filename": "example_dataset",
        })
    """

    @dg.asset(
        name=f"il_{filename}_fetch_raw_data",
    )
    def fetch_data():
        return il_fetch_bulk_data(
            file_name=filename,
        )

    return fetch_data


def il_insert_to_landing_table(dataset: dict):
    """
    Creates a Dagster asset that inserts data into a landing table
    in the PostgreSQL database.

    This asset is dependent on a corresponding fetch asset
    (which retrieves the raw data).
    It uses the provided dataset's name and columns to determine
    the landing table and perform the insert.

    Args:
        dataset (dict[str, Union[str, list[str]]]):
        A dictionary containing the dataset configuration with the following keys:
            - 'name' (str): The name of the dataset.
            - 'table_name' (str): The name of the table.
            - 'columns' (list[str]): A list of column names for the dataset.

    Returns:
        function: A Dagster asset function that inserts data
                into a landing table. It accepts a `PostgresResource` as an argument,
                  which is used for the database connection and operations.

    The insert asset performs the following:
        - It constructs the table name by appending '_landing' to the dataset name.
        - It uses a callback for data validation, ensuring each row matches the
                    expected column length.
        - It inserts the validated data into the PostgreSQL landing table via the
                    `insert_data_to_landing_table` function.

    Dependencies:
        - The asset depends on a fetch asset with the
        name `il_{dataset_name}_fetch_raw_data`.

    Example:
        il_insert_to_landing_table({
            "name": "example_dataset",
            "columns": ["column1", "column2", "column3"]
            ...
        })
    """
    fetch_asset_name = f"il_{dataset.get('name')}_fetch_raw_data"

    @dg.asset(
        deps=[fetch_asset_name],
        name=f"il_{dataset.get('name')}_insert_to_landing_table",
    )
    def insert_data(pg: dg.ResourceParam[PostgresResource]):
        dataset_name = dataset["name"]
        table_name = dataset["table_name"]
        columns = dataset["columns"]
        data_file = f"{DATA_PATH_PREFIX}/{dataset_name}.csv"

        return insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name=dataset_name,
            table_name=table_name,
            columns=columns,
            data_validation_callback=lambda row: len(row) == len(columns),
            data_file=data_file,
        )

    return insert_data


fetch_assets = [
    il_create_fetch_raw_data_assets(dataset["name"]) for dataset in DATASETS
]
insert_assets = [il_insert_to_landing_table(dataset) for dataset in DATASETS]

assets = fetch_assets + insert_assets

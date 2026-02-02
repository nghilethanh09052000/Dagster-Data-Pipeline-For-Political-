import csv
import os
import shutil
from collections.abc import Callable
from datetime import datetime
from logging import Logger
from pathlib import Path
from zipfile import ZipFile

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

NE_BASE_URL = "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads"
NE_DATA_PATH_PREFIX = "./states/nebraska"
NE_FIRST_YEAR_DATA_AVAILABLE = 2021


def download_and_extract_csv(url: str, output_dir: str, logger: Logger) -> None:
    """
    Downloads a ZIP file from the given URL, extracts the contained CSV file,
    and saves it to the specified output directory.

    Example of download urls format:

    - https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2025_ContributionLoanExtract.csv.zip

    - https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2025_ExpenditureExtract.csv.zip


    Args:
        url (str): The URL pointing to the ZIP file.
        output_dir (str): Directory to extract the CSV file into.
        logger (Logger): A logger instance used for logging progress.

    Returns:
    None: If the URL does not return a successful response (status_code != 200),
              or no valid CSV file is found.

    Notes:
        - Only the first `.csv` file found in the ZIP will be extracted.
        - The original ZIP file is deleted after extraction.
        - Existing directory structure will be created if not already present.
    """
    logger.info(f"Start downloading for url {url}... ")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    zip_path = Path(output_dir) / Path(url).name
    csv_filename = zip_path.with_suffix("").with_suffix(".csv")

    stream_download_file_to_path(url, zip_path)

    try:
        with ZipFile(zip_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith(".csv"):
                    with (
                        zip_ref.open(file_info) as source,
                        open(csv_filename, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)
    finally:
        os.remove(zip_path)


def ne_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    category: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Inserts raw CSV data from previously downloaded files into the specified
    PostgreSQL landing table.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL,
                                    typically injected from a Dagster resource.

        category (str): The subdirectory under each year
                        (e.g., 'ContributionLoanExtract'),
                        used to locate CSV files inside
                        `states/nebraska/{year}/{category}`.

        table_name (str): Name of the landing table to truncate and insert.

        table_columns_name (List[str]): List of column names in the
                                        correct order for insert.

        data_validation_callback (Callable): A function that takes a CSV row and returns
                                    a boolean indicating if the row should be inserted.

    Returns:
        MaterializeResult: A Dagster metadata result object containing
                            the table name and the number of rows inserted.

    Process:
        - Scans folders from 2014 to the current year for CSVs in the target category.
        - Truncates the destination table before inserting new data.
        - Validates and inserts data row-by-row using the provided validation function.
        - Logs all steps and errors.
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    current_year = datetime.now().year
    base_path = Path(NE_DATA_PATH_PREFIX)
    data_files = []

    for year in range(NE_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        glob_path = base_path / str(year) / category
        if glob_path.exists():
            csvs = list(glob_path.glob("*.csv"))
            data_files.extend([str(csv) for csv in csvs])

    logger.info(f"Category: {category} | Found {len(data_files)} files.")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name}.")
        pg_cursor.execute(query=truncate_query)

        try:
            for data_file in data_files:
                if not os.path.exists(data_file):
                    logger.warning(f"File {data_file} does not exist, skipping...")
                    continue

                logger.info(f"Inserting from: {data_file}")

                data_type_lines_generator = safe_readline_csv_like_file(
                    data_file,
                    encoding="utf-8",
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=",", quotechar='"'
                )

                next(parsed_data_type_file)  # Skip header

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_data_type_file,
                    table_name=table_name,
                    table_columns_name=table_columns_name,
                    row_validation_callback=data_validation_callback,
                )

        except Exception as e:
            logger.error(f"Error while processing {category}: {e}")
            raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        row_count = int(count_cursor_result[0]) if count_cursor_result else 0

        logger.info(f"Inserted {row_count} rows into {table_name}")

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset
def ne_contributions_loans_download_data() -> None:
    """
    Downloads and extracts nebraska contributions and loans CSV data.
    This asset fetches the data from a URL, extracts the CSV files, and saves them
    to the specified output directory.
    """
    logger = dg.get_dagster_logger(name="ne_contributions_loans_download_data")
    current_year = datetime.now().year
    for year in range(NE_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        url = f"{NE_BASE_URL}/{year}_ContributionLoanExtract.csv.zip"
        output_dir = f"states/nebraska/{year}/ContributionLoanExtract"
        download_and_extract_csv(url, output_dir, logger)


@dg.asset
def ne_expenditures_download_data() -> None:
    """
    Downloads and extracts nebraska expenditures CSV data.
    This asset fetches the data from a URL, extracts the CSV files, and saves them
    to the specified output directory.
    """
    logger = dg.get_dagster_logger(name="ne_expenditures_download_data")
    current_year = datetime.now().year
    for year in range(NE_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        url = f"{NE_BASE_URL}/{year}_ExpenditureExtract.csv.zip"
        output_dir = f"states/nebraska/{year}/ExpenditureExtract"
        download_and_extract_csv(url, output_dir, logger)


@dg.asset(deps=[ne_contributions_loans_download_data])
def ne_insert_contributions_loans(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts contributions and loans data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ne_contributions_loan_landing' table
    """
    return ne_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="ContributionLoanExtract",
        table_name="ne_contributions_loan_landing",
        table_columns_name=[
            "receipt_id",
            "org_id",
            "filer_type",
            "filer_name",
            "candidate_name",
            "receipt_transaction_contribution_type",
            "other_funds_type",
            "receipt_date",
            "receipt_amount",
            "description",
            "contributor_or_transaction_source_type",
            "contributor_or_source_last_name",
            "first_name",
            "middle_name",
            "suffix",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "filed_date",
            "amended",
            "employer",
            "occupation",
        ],
        data_validation_callback=lambda row: len(row) == 24,
    )


@dg.asset(deps=[ne_expenditures_download_data])
def ne_insert_expenditures(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts expenditure data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ne_expenditures_landing' table
    """
    return ne_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="ExpenditureExtract",
        table_name="ne_expenditures_landing",
        table_columns_name=[
            "expenditure_id",
            "org_id",
            "filer_type",
            "filer_name",
            "candidate_name",
            "expenditure_transaction_type",
            "expenditure_sub_type",
            "expenditure_date",
            "expenditure_amount",
            "description",
            "contributor_type",
            "contributor_name",
            "first_name",
            "middle_name",
            "suffix",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "filed_date",
            "support_or_oppose",
            "ballot_issue_or_candidate_name",
            "jurisdiction_description",
            "amended",
            "employer",
            "occupation",
            "principal_place_of_business",
        ],
        data_validation_callback=lambda row: len(row) == 28,
    )

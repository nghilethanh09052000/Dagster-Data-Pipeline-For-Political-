import csv
from datetime import datetime
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import insert_parsed_file_to_landing_table
from contributions_pipeline.resources import PostgresResource

RI_DATA_BASE_PATH = "./states/rhode_island"
RI_SUPABASE_STORAGE_URL = "https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/ri/"
RI_FIRST_YEAR_DATA_AVAILABLE = 2002


@dg.asset(name="ri_download_all_expenditures")
def download_all_expenditures(context: AssetExecutionContext):
    """
    Download all Rhode Island expenditure CSV files from a public
    Supabase storage.
    This asset automates the process of retrieving annual expenditure
    records for Rhode Island by downloading CSV files from 2002 up to
    the current year. Files are saved locally under the
    'states/rhode_island' directory.
    Each file is named `expenditures_<year>.csv`. If the target
    directory does not exist, it is created. All download attempts are
    logged via the Dagster context.
    Returns:
        List[Path]: A list of `Path` objects pointing to the
        downloaded files.
    """

    last_year_inclusive = datetime.now().year

    for year in range(RI_FIRST_YEAR_DATA_AVAILABLE, last_year_inclusive + 1):
        filename = f"expenditures_{year}.csv"
        output_path = Path(RI_DATA_BASE_PATH) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Downloading {filename}")
        stream_download_file_to_path(
            request_url=f"{RI_SUPABASE_STORAGE_URL}{filename}",
            file_save_path=output_path,
        )


@dg.asset(name="ri_insert_all_expenditures", deps=[download_all_expenditures])
def insert_all_expenditures(
    context: AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all downloaded Rhode Island expenditure data into a PostgreSQL
    landing table.
    This asset reads each CSV file (from 2002 to the current year)
    previously downloaded to the local directory, parses its contents
    while skipping the header row, and inserts records into a PostgreSQL
    table named `ri_expenditures_landing`.
    The CSV files are expected to have 21 columns, and rows not matching
    this column count will be ignored. Insertions are done via a utility
    function that handles row-level validation and formatting.
    Parameters:
        context (AssetExecutionContext): Dagster execution context
        for logging.
        pg (PostgresResource): Dagster-provided Postgres
        connection resource.
    Notes:
        - Assumes each file is named `expenditures_<year>.csv`.
        - If a file is missing, it will be skipped with a warning.
        - The connection is reused across all files for performance.
    """
    with pg.pool.connection() as conn, conn.cursor() as cursor:
        last_year_inclusive = datetime.now().year

        for year in range(RI_FIRST_YEAR_DATA_AVAILABLE, last_year_inclusive + 1):
            file_path = Path(RI_DATA_BASE_PATH) / f"expenditures_{year}.csv"

            if not file_path.exists():
                context.log.warning(
                    f"File not found: {file_path}. Skipping year {year}."
                )
                continue

            context.log.info(f"Processing {file_path}")

            file_lines = safe_readline_csv_like_file(file_path)
            parsed_file = csv.reader(file_lines, delimiter=",", quotechar='"')
            # Skip header
            next(parsed_file, None)

            insert_parsed_file_to_landing_table(
                pg_cursor=cursor,
                csv_reader=parsed_file,
                table_name="ri_expenditures_landing",
                table_columns_name=[
                    "OrganizationName",
                    "ExpenditureID",
                    "DisbDesc",
                    "ExpDesc",
                    "ExpPmtDesc",
                    "IncompleteDesc",
                    "ViewIncomplete",
                    "ExpDate",
                    "PmtDate",
                    "Amount",
                    "FullName",
                    "Address",
                    "CityStZip",
                    "ReceiptDesc",
                    "ExpenditureCodeID",
                    "BeginDate",
                    "EndDate",
                    "MPFUsed",
                    "OSAP",
                    "ZeroedByCF7",
                    "RICF7FilingId",
                ],
                row_validation_callback=lambda row: len(row) == 21,
            )


@dg.asset(name="ri_download_contributions")
def download_all_contributions(context: AssetExecutionContext):
    """
    Download Rhode Island contributions CSV file from public Supabase storage.
    This asset downloads a single file containing all contribution records.
    File is saved locally under the 'states/rhode_island' directory.
    Returns:
        List[Path]: List containing the path to the downloaded file.
    """

    last_year_inclusive = datetime.now().year

    for year in range(RI_FIRST_YEAR_DATA_AVAILABLE, last_year_inclusive + 1):
        filename = f"contributions_{year}.csv"
        output_path = Path(RI_DATA_BASE_PATH) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Downloading {filename}")
        stream_download_file_to_path(
            request_url=f"{RI_SUPABASE_STORAGE_URL}{filename}",
            file_save_path=output_path,
        )


@dg.asset(name="ri_insert_contributions", deps=[download_all_contributions])
def insert_contributions(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    with pg.pool.connection() as conn, conn.cursor() as cursor:
        last_year_inclusive = datetime.now().year

        for year in range(RI_FIRST_YEAR_DATA_AVAILABLE, last_year_inclusive + 1):
            file_path = Path(RI_DATA_BASE_PATH) / f"contributions_{year}.csv"

            if not file_path.exists():
                context.log.warning(
                    f"File not found: {file_path}. Skipping year {year}."
                )
                continue

            context.log.info(f"Processing {file_path}")

            raw_lines = (
                line for line in safe_readline_csv_like_file(file_path) if line.strip()
            )
            parsed_file = csv.reader(raw_lines, delimiter=",", quotechar='"')
            next(parsed_file, None)  # Skip header

            insert_parsed_file_to_landing_table(
                pg_cursor=cursor,
                csv_reader=parsed_file,
                table_name="ri_contributions_landing",
                table_columns_name=[
                    "ContributionID",
                    "ContDesc",
                    "IncompleteDesc",
                    "OrganizationName",
                    "ViewIncomplete",
                    "ReceiptDate",
                    "DepositDate",
                    "Amount",
                    "ContribExplanation",
                    "MPFMatchAmount",
                    "FirstName",
                    "LastName",
                    "FullName",
                    "Address",
                    "CityStZip",
                    "EmployerName",
                    "EmpAddress",
                    "EmpCityStZip",
                    "ReceiptDesc",
                    "BeginDate",
                    "EndDate",
                    "TransType",
                ],
                row_validation_callback=lambda row: len(row) == 22,
            )
            context.log.info("Insertion finished with valid cleaned rows")

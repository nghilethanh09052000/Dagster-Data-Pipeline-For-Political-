import csv
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

MD_DATA_PATH_PREFIX = "./states/maryland"
MD_FIRST_YEAR_DATA_AVAILABLE = 1998

MD_SUPABASE_STORAGE_BASE_URL = "https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/md"

MD_CONTRIBUTIONS_AND_LOANS_TABLE_NAME = "md_contributions_and_loans_landing"
MD_CONTRIBUTIONS_AND_LOANS_TABLE_COLUMNS = [
    "ReceivingCommittee",
    "FilingPeriod",
    "ContributionDate",
    "ContributorName",
    "ContributorAddress",
    "ContributorType",
    "ContributionType",
    "ContributionAmount",
    "EmployerName",
    "EmployerOccupation",
    "Office",
    "Fundtype",
    "Unused",
]
MD_CONTRIBUTIONS_AND_LOANS_FILE_NAME = "ContributionsList.csv"


def md_fetch_all_files_from_storage(base_url: str, start_year: int):
    """
    This funciton is going to dowload all the files from a static download site.
    The expected URL format is "{base_url}/{year}/ContributionsList.csv".
    If there's 404 on any of the file_name, the function will only logs it,
    no error will be raised.

    Params:
    base_url: base url of the storage
    start_year: the first year you want to check for all of the file names in the
            base url, this will end in the current year and month
    """

    logger = dg.get_dagster_logger("md_fetch_from_storage")
    base_path = Path(MD_DATA_PATH_PREFIX)

    last_year_inclusive = datetime.now().year

    for year in range(start_year, last_year_inclusive + 1):
        year_data_base_path = base_path / str(year)
        year_data_base_path.mkdir(parents=True, exist_ok=True)

        with requests.session() as req_session:
            file_download_uri = (
                f"{base_url}/{year}/{MD_CONTRIBUTIONS_AND_LOANS_FILE_NAME}"
            )

            logger.info(f"Downloading on year {year} ({file_download_uri})")

            with req_session.get(file_download_uri, stream=True) as response:
                if response.status_code == 404 or response.status_code == 400:
                    logger.warning(
                        f"File on year {year} not found (URL: {file_download_uri}"
                    )
                    continue

                response.raise_for_status()

                file_save_path = (
                    year_data_base_path / MD_CONTRIBUTIONS_AND_LOANS_FILE_NAME
                )
                logger.info(f"Saving on year {year} to {file_save_path}")

                with open(file_save_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)


@dg.asset()
def md_contributions_and_loans():
    """
    Fetch all the manually scraped contributions and loans data from Supabase Storage.
    """

    md_fetch_all_files_from_storage(
        base_url=MD_SUPABASE_STORAGE_BASE_URL,
        start_year=MD_FIRST_YEAR_DATA_AVAILABLE,
    )


@dg.asset(deps=[md_contributions_and_loans])
def md_insert_contributions_and_loans(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all the fetched contributions and loans data to landing table.
    """

    base_path = Path(MD_DATA_PATH_PREFIX)

    last_year_inclusive = datetime.now().year

    truncate_query = get_sql_truncate_query(
        table_name=MD_CONTRIBUTIONS_AND_LOANS_TABLE_NAME
    )

    with (
        pg.pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        context.log.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for year in range(MD_FIRST_YEAR_DATA_AVAILABLE, last_year_inclusive + 1):
            current_year_raw_data_path = (
                base_path / f"{year}/{MD_CONTRIBUTIONS_AND_LOANS_FILE_NAME}"
            )

            if (
                not current_year_raw_data_path.exists()
                or not current_year_raw_data_path.is_file()
            ):
                context.log.warning(
                    f"Path {current_year_raw_data_path} is either not a file or doesn't"
                    " exists, ignoring..."
                )
                continue

            try:
                current_year_file_lines_generator = safe_readline_csv_like_file(
                    file_path=current_year_raw_data_path,
                    encoding="utf-8",
                )
                parsed_current_year_file = csv.reader(
                    current_year_file_lines_generator,
                    delimiter=",",
                    quoting=csv.QUOTE_MINIMAL,
                )

                # Skip the header
                next(parsed_current_year_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_current_year_file,
                    table_name=MD_CONTRIBUTIONS_AND_LOANS_TABLE_NAME,
                    table_columns_name=MD_CONTRIBUTIONS_AND_LOANS_TABLE_COLUMNS,
                    row_validation_callback=lambda row: len(row)
                    == len(MD_CONTRIBUTIONS_AND_LOANS_TABLE_COLUMNS),
                )

            except FileNotFoundError:
                context.log.warning(
                    f"File for year {year} is non-existent, ignoring..."
                )
            except Exception as e:
                context.log.error(
                    f"Got error while reading cycle year {year} file: {e}"
                )
                raise e

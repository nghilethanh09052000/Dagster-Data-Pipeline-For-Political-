import csv
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Literal

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

GEORGIA_BASE_PATH = "./states/georgia"
GEORGIA_FIRST_YEAR_DATA_AVAILABLE = 2021
# Georgia ethics site basically just loads until it timed-out,
# the only data export that really works is 2021 only.
# TODO: find other way to get the data? ask their sysadmin to fix the perf issue
# on export? hell, ask to dump their database if we can
GEORGIA_LAST_RELIABLE_YEAR = 2021
GEORGIA_REQUEST_TIMEOUT_SECONDS = 3 * 60 * 60


def ga_get_data_export_stream_url_for_year_and_type(
    year: int, ga_report_data_type: Literal["CON", "EXP"]
) -> str:
    """
    Get data stream from Georgia Ethics Website. The efile api is used for the data
    download, but when done through `curl` for testing, the server initated a
    data stream instead.

    Parameters:
    year: the year you want to get the data
    ga_report_data_type: CON for "Contributions and Loans", EXP for "Expenditures"
    """
    return f"https://efile.ethics.ga.gov/api/DataDownload/GetCSVDownloadReport?year={year}&transactionType={ga_report_data_type}&reportFormat=csv&fileName={ga_report_data_type}_{year}.csv&filerId=0"


def ga_fetch_data_on_all_years_of_one_type(
    start_year: int, ga_report_data_type: Literal["CON", "EXP"]
):
    """
    Stream data to "./states/gerogia/{year}/{data_type}.csv" from Georgia ethics
    efile API.

    Parameters:
    year: the first year the data is available, inclusive
    ga_report_data_type: CON for "Contributions and Loans", EXP for "Expenditures"
    """

    logger = dg.get_dagster_logger(f"ga_{ga_report_data_type}_fetch")

    base_path = Path(GEORGIA_BASE_PATH)

    end_year_inclusive = GEORGIA_LAST_RELIABLE_YEAR

    for year in range(start_year, end_year_inclusive + 1):
        data_stream_url = ga_get_data_export_stream_url_for_year_and_type(
            year=year, ga_report_data_type=ga_report_data_type
        )
        year_data_dir = base_path / str(year)
        year_data_dir.mkdir(parents=True, exist_ok=True)

        year_raw_data_path = year_data_dir / f"{ga_report_data_type}.csv"

        logger.info(
            (
                f"Starting to stream {year} data {data_stream_url}",
                f"Raw data will be streamed to {year_raw_data_path}",
            )
        )

        with requests.get(
            data_stream_url, stream=True, timeout=GEORGIA_REQUEST_TIMEOUT_SECONDS
        ) as stream_req:
            stream_req.raise_for_status()

            with open(year_raw_data_path, "wb") as raw_data_file:
                # Use 64KB Buffer
                for chunk in stream_req.iter_content(chunk_size=65536):
                    raw_data_file.write(chunk)

            # Each of the request took minutes to handle before the backend could return
            # anything. So better wait for a little bit so we don't overload it.
            time.sleep(30)


def ga_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
    ga_report_data_type: Literal["CON", "EXP"],
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Insert all available data files from one report type to its repsective postgres
    landing table. The function will ignore all missing year.

    This function assumed that all of the files have the same columns.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.
    start_year: the first year the data is available,
    ga_report_data_type: CON for "Contributions and Loans", EXP for "Expenditures"
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for individual contributions.
                        ["cmte_id", "amndt_ind", "rpt_tp,transaction_pgi", "image_num",
                         "transaction_tp", ..., "sub_id"]
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    """

    logger = dg.get_dagster_logger(f"ga_{ga_report_data_type}_insert")

    base_path = Path(GEORGIA_BASE_PATH)

    truncate_query = get_sql_truncate_query(table_name=table_name)

    first_year = start_year
    last_year_inclusive = datetime.now().year

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for year in range(first_year, last_year_inclusive + 1):
            current_year_data_file_path = (
                base_path / f"{year}/{ga_report_data_type}.csv"
            )

            logger.info(
                f"Inserting year {year} file to pg ({current_year_data_file_path})"
            )

            try:
                current_year_file_lines_generator = safe_readline_csv_like_file(
                    file_path=current_year_data_file_path
                )
                parsed_current_cycle_file = csv.reader(
                    current_year_file_lines_generator,
                    delimiter=",",
                    quotechar='"',
                )

                # Skip the headers
                next(parsed_current_cycle_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_current_cycle_file,
                    table_name=table_name,
                    table_columns_name=table_columns_name,
                    row_validation_callback=data_validation_callback,
                )

            except FileNotFoundError:
                logger.warning(f"File for year {year} is non-existent, ignoring...")
            except Exception as e:
                logger.error(f"Got error while reading cycle year {year} file: {e}")
                raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset()
def ga_contributions_and_loans():
    """
    Fetch all contributions and loans ("CON") data from first year available
    until the current year.
    """

    ga_fetch_data_on_all_years_of_one_type(
        start_year=GEORGIA_FIRST_YEAR_DATA_AVAILABLE, ga_report_data_type="CON"
    )


@dg.asset(deps=[ga_contributions_and_loans])
def ga_contributions_and_loans_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all the available contributions and loans ("CON") raw data to landing table
    from the first year available until the current year.
    """

    ga_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=GEORGIA_FIRST_YEAR_DATA_AVAILABLE,
        ga_report_data_type="CON",
        table_name="ga_contributions_and_loans_landing",
        table_columns_name=[
            "Filer_ID",
            "Transaction_Amount",
            "Transaction_Date",
            "Last_Name",
            "First_Name",
            "Middle_Name",
            "Prefix",
            "Suffix",
            "Contributor_Address_Line_1",
            "Contributor_Address_Line_2",
            "Contributor_City",
            "Contributor_State",
            "Contributor_Zip_Code",
            "Description",
            "Check_Number",
            "Transaction_ID",
            "Filed_Date",
            "Election",
            "Report_Name",
            "Start_of_Period",
            "End_of_Period",
            "Contributor_Code",
            "Contribution_Type",
            "Report_Entity_Type",
            "Committee_Name",
            "Candidate_Last_Name",
            "Candidate_First_Name",
            "Candidate_Middle_Name",
            "Candidate_Prefix",
            "Candidate_Suffix",
            "Amended",
            "Contributor_Employer",
            "Contributor_Occupation",
            "Occupation_Comment",
            "Employment_Information_Requested",
        ],
        data_validation_callback=lambda row: len(row) == 35,
    )


@dg.asset()
def ga_expenditures():
    """
    Fetch all expenditures ("EXP") data from first year available
    until the current year.
    """

    ga_fetch_data_on_all_years_of_one_type(
        start_year=GEORGIA_FIRST_YEAR_DATA_AVAILABLE, ga_report_data_type="EXP"
    )


@dg.asset(deps=[ga_expenditures])
def ga_expenditures_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all the available expenditures ("EXP") raw data to landing table
    from the first year available until the current year.
    """

    ga_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=GEORGIA_FIRST_YEAR_DATA_AVAILABLE,
        ga_report_data_type="EXP",
        table_name="ga_expenditures_landing",
        table_columns_name=[
            "Filer_ID",
            "Expenditure_Amount",
            "Expenditure_Date",
            "Payee_Last_Name",
            "Payee_First_Name",
            "Payee_Middle_Name",
            "Payee_Prefix",
            "Payee_Suffix",
            "Payee_Address_1",
            "Payee_Address_2",
            "Payee_City",
            "Payee_State",
            "Payee_Zip_Code",
            "Description",
            "Expenditure_ID",
            "Filed_Date",
            "Election",
            "Report_Name",
            "Start_of_Period",
            "End_of_Period",
            "Purpose",
            "Expenditure_Type",
            "Reason",
            "Stance",
            "Report_Entity_Type",
            "Committee_Name",
            "Candidate_Last_Name",
            "Candidate_First_Name",
            "Candidate_Middle_Name",
            "Candidate_Prefix",
            "Candidate_Suffix",
            "Amended",
        ],
        data_validation_callback=lambda row: len(row) == 32,
    )

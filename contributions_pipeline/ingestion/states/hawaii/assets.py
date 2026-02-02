import csv
import os
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

BASE_URL = "https://hicscdata.hawaii.gov/api/views"
HI_DATA_PATH = f"{os.getcwd()}/.states/hawaii"

REPORT_PARAMS = {
    "CONTRIB_RECV_BY_CANDIDATE": {"fourfour": "jexd-xbcg", "cacheBust": "1738886088"},
    "EXPENDITURE_MADE_BY_CANDIDATE": {
        "fourfour": "3maa-4fgr",
        "cacheBust": "1740440243",
    },
    "CONTRIB_MADE_TO_CANDIDATE": {"fourfour": "6huc-dcuw", "cacheBust": "1738888310"},
    "CONTRIB_RECV_BY_NON_CANDIDATES": {
        "fourfour": "rajm-32md",
        "cacheBust": "1738887882",
    },
    "EXPENDITURE_MADE_BY_NON_CANDIDATE": {
        "fourfour": "riiu-7d4b",
        "cacheBust": "1738888035",
    },
    "LOANS_RECV_BY_CANDIDATE": {"fourfour": "yf4f-x3r4", "cacheBust": "1738886561"},
}


def get_url_for_hawaii_bulk_download_one_category(**file_param) -> str:
    """
    Get hawaii bulk download URL for specific prefix (report type) for a cycle
    """

    download_date = datetime.now().strftime("%Y%m%d")

    return (
        f"{BASE_URL}/{file_param['fourfour']}"
        + f"/rows.csv?fourfour={file_param['fourfour']}"
        + f"&cacheBust={file_param['cacheBust']}"
        + f"&date={download_date}&accessType=DOWNLOAD"
    )


def hi_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    data_file_path: str,
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
    start_year: the first year (the latest of the cycle year) the data is available,
                e.g. for candidate master is 1979-1980, so put in 1980
    data_file_path: the file path of the actual data file on the year folder, for
                    example contributions, the local folder for it is laid out as it
                    follows (year) -> contrib.txt. The data file is itcont.txt,
                    thus the input should be "/contrib.txt".
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted.
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    """

    logger = dg.get_dagster_logger(f"hi_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(HI_DATA_PATH)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        current_cycle_data_file_path = base_path / f"{data_file_path}"

        logger.info(f"Inserting file to pg ({current_cycle_data_file_path})")

        try:
            current_cycle_file_lines_generator = safe_readline_csv_like_file(
                file_path=current_cycle_data_file_path, encoding="utf-8"
            )
            parsed_current_cycle_file = csv.reader(
                current_cycle_file_lines_generator,
                delimiter=",",
                quotechar='"',
            )

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_current_cycle_file,
                table_name=table_name,
                table_columns_name=table_columns_name,
                row_validation_callback=data_validation_callback,
            )

        except FileNotFoundError:
            logger.warning(f"File for {data_file_path} is non-existent, ignoring...")
        except Exception as e:
            logger.error(f"Got error while reading {data_file_path} file: {e}")
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
def hi_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all hawaii finance campaign report.
    """

    file_path = Path(HI_DATA_PATH)
    file_path.mkdir(parents=True, exist_ok=True)

    for report_type, url_params in REPORT_PARAMS.items():
        context.log.info(f"Downloading campaign full export data for {report_type}")

        url = get_url_for_hawaii_bulk_download_one_category(**url_params)

        try:
            stream_download_file_to_path(
                request_url=url, file_save_path=file_path / f"{report_type}.csv"
            )

        except FileNotFoundError:
            context.log.info(f"File for {report_type} is non-existent, ignoring... ")
        except Exception as e:
            context.log.info(f"Got error while reading {report_type} file: {e}")
            raise e


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_contrib_made_to_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI CONTRIB_MADE_TO_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="CONTRIB_MADE_TO_CANDIDATE.csv",
        table_name="hi_contrib_made_to_candidate_landing_table",
        table_columns_name=[
            "Noncandidate Committee Name",
            "Candidate Name",
            "CC Reg No",
            "Candidate Committee Name",
            "Date",
            "Amount",
            "Aggregate",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Non-Monetary (Yes or No)",
            "Non-Monetary Category",
            "Non-Monetary Description",
            "Reg No",
            "Election Period",
            "Mapping Location",
            "Office",
            "District",
            "County",
            "Party",
        ],
        data_validation_callback=lambda row: len(row) == 22,
    )


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_contrib_recv_by_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI CONTRIB_RECV_BY_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="CONTRIB_RECV_BY_CANDIDATE.csv",
        table_name="hi_contrib_recv_by_candidate_landing_table",
        table_columns_name=[
            "Candidate Name",
            "Contributor Type",
            "Contributor Name",
            "Date",
            "Amount",
            "Aggregate",
            "Employer",
            "Occupation",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Non-Resident (Yes or No)",
            "Non-Monetary (Yes or No)",
            "Non-Monetary Category",
            "Non-Monetary Description",
            "Office",
            "District",
            "County",
            "Party",
            "Reg No",
            "Election Period",
            "Mapping Location",
            "InOutState",
            "Range",
        ],
        data_validation_callback=lambda row: len(row) == 26,
    )


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_contrib_recv_by_non_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI CONTRIB_RECV_BY_NON_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="CONTRIB_RECV_BY_NON_CANDIDATES.csv",
        table_name="hi_contrib_recv_by_non_candidates_landing_table",
        table_columns_name=[
            "Noncandidate Committee Name",
            "Contributor Type",
            "Contributor Name",
            "Date",
            "Amount",
            "Aggregate",
            "Employer",
            "Occupation",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Non-Monetary (Yes or No)",
            "Non-Monetary Category",
            "Non-Monetary Description",
            "Reg No",
            "Election Period",
            "Mapping Location",
        ],
        data_validation_callback=lambda row: len(row) == 19,
    )


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_expenditure_made_by_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI EXPENDITURE_MADE_BY_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="EXPENDITURE_MADE_BY_CANDIDATE.csv",
        table_name="hi_expenditure_made_by_candidate_landing_table",
        table_columns_name=[
            "Candidate Name",
            "Vendor Type",
            "Vendor Name",
            "Date",
            "Amount",
            "Authorized Use",
            "Expenditure Category",
            "Purpose of Expenditure",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Office",
            "District",
            "County",
            "Party",
            "Reg No",
            "Election Period",
            "Mapping Location",
            "InOutState",
        ],
        data_validation_callback=lambda row: len(row) == 21,
    )


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_expenditure_made_by_non_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI EXPENDITURE_MADE_BY_NON_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="EXPENDITURE_MADE_BY_NON_CANDIDATE.csv",
        table_name="hi_expenditure_made_by_non_candidate_landing_table",
        table_columns_name=[
            "Noncandidate Committee Name",
            "Vendor Type",
            "Vendor Name",
            "Date",
            "Amount",
            "Expenditure Category",
            "Purpose of Expenditure",
            "Independent Expenditure",
            "Candidate Name(s)",
            "Support/Oppose",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Reg No",
            "Election Period",
            "Mapping Location",
        ],
        data_validation_callback=lambda row: len(row) == 18,
    )


@dg.asset(deps=[hi_fetch_all_campaign_data_full_export])
def hi_insert_loans_recv_by_candidate_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format HI LOANS_RECV_BY_CANDIDATE.csv data
    to the right landing table"""

    hi_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="LOANS_RECV_BY_CANDIDATE.csv",
        table_name="hi_loans_recv_by_candidate_landing_table",
        table_columns_name=[
            "Candidate Name",
            "Lender Type",
            "Lender Name",
            "Date",
            "Loan Amount",
            "Loan Type",
            "Loan Source",
            "Purpose of Loan",
            "Repay Amount",
            "Loan ID",
            "Forgiven",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip Code",
            "Office",
            "District",
            "County",
            "Party",
            "Reg No",
            "Election Period",
            "Mapping Location",
        ],
        data_validation_callback=lambda row: len(row) == 23,
    )

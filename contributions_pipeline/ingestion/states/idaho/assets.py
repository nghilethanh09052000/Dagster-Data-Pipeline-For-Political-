"""
Idaho Ingestion Assets

There's quite a complex schema system. The new system (2020 onwards) uses good
and clean csv files. The older system outputs xls(x) that's just slightly
different from eachother.

See the README in this folder for better look at the mappings.

"""

import csv
import datetime
import os
import time
from collections.abc import Callable
from pathlib import Path

import dagster as dg
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import (
    download_file_from_http,
    fetch_url_with_chrome_user_agent,
    get_all_links_from_html,
    save_response_as_file,
)
from contributions_pipeline.lib.file import (
    safe_readline_csv_like_file,
    save_parse_excel,
)
from contributions_pipeline.lib.ingest import (
    COPY_INSERT_BATCH_NUM,
    LOG_INVALID_ROWS,
    get_sql_binary_format_copy_query_for_landing_table,
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.lib.iter import batched
from contributions_pipeline.resources import PostgresResource

# Constants for Idaho contribution data
ID_DATA_BASE_PATH = "./states/idaho"

ID_OLD_BASE_URL = "https://sos.idaho.gov/elect/finance/downloads.html"
ID_NEW_BASE_URL = (
    "https://api-sunshine.voteidaho.gov/api/ExportData/GetExportPublicDownloadData"
)

ID_NEW_CONTRIBUTION_COLUMNS = [
    "filing_entity_id",
    "filing_entity_name",
    "campaign_name",
    "registration_type",
    "transaction_id",
    "transaction_type",
    "transaction_sub_type",
    "contributor_type",
    "contributor_last_name",
    "contributor_first_name",
    "contributor_address_line_1",
    "contributor_address_line_2",
    "contributor_address_city",
    "contributor_address_state",
    "contributor_address_zip_code",
    "transaction_date",
    "transaction_amount",
    "loan_interest_amount",
    "total_loan_amount",
    "election_type",
    "election_year",
    "transaction_description",
    "amended",
    "timed_report_name",
    "timed_report_date",
    "report_name",
    "report_filed_date",
]

ID_FIRST_YEAR_OF_OLD_DATA_AVAILABLE = 1994
ID_LAST_YEAR_OF_OLD_DATA_AVAILABLE = 2019

ID_FIRST_YEAR_OF_NEW_DATA_AVAILABLE = ID_LAST_YEAR_OF_OLD_DATA_AVAILABLE + 1


def get_result_directory_path_for_id_one_year_bulk_download(year_month: str) -> str:
    """
    Get the result directory for a specific year_month (could be year or year_month
    format depending if year is under 1994 or after).

    Parameters
    ----------
        year_month: (str)
                    string with "year" or "year_month" format, e.g. "1999", "2020_01",
                    "2003_12", etc.

    """

    return f"{ID_DATA_BASE_PATH}/{year_month}"


def id_fetch_old_report_type(base_url: str) -> None:
    logger = dg.get_dagster_logger("id_fetch_old_report_type")

    base_page_request = fetch_url_with_chrome_user_agent(url=base_url)
    if not base_page_request.ok:
        raise RuntimeError(
            (
                f"Failed to fetch base page: {base_url}",
                f"response code: {base_page_request.status_code}",
                f"response body: {base_page_request.text}",
                f"response header: {base_page_request.headers}",
            )
        )

    base_page = base_page_request.text
    download_urls = get_all_links_from_html(
        page_source=base_page, base_url="https://sos.idaho.gov/elect/finance/"
    )

    logger.info(
        (
            f"Got sub-page links from base page: {download_urls}",
            f"Raw Page: {base_page}",
        )
    )

    for link in download_urls:
        # file naming
        report_year_month, file_name = link.split("/")[-2:]
        logger.info(
            {
                "message": "Getting Idaho election finance bulk download",
                "url": link,
                "report_year": report_year_month,
            }
        )

        # Only process data start from 1994 - 2019
        if int(report_year_month) <= ID_LAST_YEAR_OF_OLD_DATA_AVAILABLE:
            report_year_month_dir = (
                get_result_directory_path_for_id_one_year_bulk_download(
                    year_month=report_year_month
                )
            )
            Path(report_year_month_dir).mkdir(parents=True, exist_ok=True)

            if "%20" in file_name:
                file_name = file_name.replace("%20", "")

            report_file_path = f"{report_year_month_dir}/{file_name}"

            if not os.path.exists(report_file_path):
                response = fetch_url_with_chrome_user_agent(link)
                while response.status_code != 200:
                    time.sleep(1)
                    response = fetch_url_with_chrome_user_agent(link)

                save_response_as_file(
                    response=response, destination_file_path=report_file_path
                )


def id_fetch_new_report_type(base_url: str) -> None:
    logger = dg.get_dagster_logger("id_fetch_new_report_type")

    last_year_inclusive = datetime.datetime.now().year

    # Since new report URLs are obtained from the API,
    # set the fetch period from 2020 to now directly.
    for year_month in range(
        ID_FIRST_YEAR_OF_NEW_DATA_AVAILABLE, last_year_inclusive + 1
    ):
        report_year_month_dir = get_result_directory_path_for_id_one_year_bulk_download(
            year_month=str(year_month)
        )
        Path(report_year_month_dir).mkdir(parents=True, exist_ok=True)

        report_file_path = f"{report_year_month_dir}/data.csv"
        try:
            if not os.path.exists(report_file_path):
                request_body = {
                    "transactionTypeCode": "TCON",
                    "type": "CSV",
                    "filingYear": year_month,
                    "openInNewTab": False,
                }
                download_file_from_http(
                    request_url=base_url,
                    request_body=request_body,
                    file_save_path=report_file_path,
                    method="POST",
                )
        except Exception as e:
            logger.error(f"Got error while fetching new report type: {e}")


def id_fetch_all_files_of_all_report_type_all_time(
    base_url: str, report_type: str
) -> None:
    """
    Main function to fetch all files of idaho all report types for all time.
    """
    if report_type == "old":
        id_fetch_old_report_type(base_url)
    elif report_type == "new":
        id_fetch_new_report_type(base_url)
    else:
        raise ValueError(f"Invalid report type: {report_type}")


def id_insert_files_to_postgres(
    postgres_pool: ConnectionPool,
    start_year: int,
    last_year_inclusive: int,
    table_name: str,
    table_columns_name: list[str],
    report_type: str,
    truncate: bool = False,
    filter_file: Callable[[str], bool] = lambda _: True,
) -> dg.MaterializeResult:
    logger = dg.get_dagster_logger(f"id_insert_{report_type}")
    truncate_query = get_sql_truncate_query(table_name=table_name)
    landing_table_copy_query = get_sql_binary_format_copy_query_for_landing_table(
        table_name=table_name, table_columns_name=table_columns_name
    )

    base_path = Path(ID_DATA_BASE_PATH)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        if truncate:
            logger.info("Truncating table before inserting the new one")
            pg_cursor.execute(query=truncate_query)

        # Read files
        for year in range(start_year, last_year_inclusive + 1):
            dir_path = base_path / str(year)

            # Check if the current item is a directory and exists
            if not dir_path.exists() or not os.path.isdir(dir_path):
                logger.warning(f"Folder path {dir_path} non-existent, ignoring...")
                continue

            try:
                for filename in os.listdir(dir_path):
                    if not filter_file(filename):
                        logger.warning(
                            f"File {filename} is ignored as the filter file function"
                            " doesn't return True"
                        )
                        continue

                    # Excel file (old type)
                    if report_type == "old" and (
                        filename.endswith(".xls") or filename.endswith(".xlsx")
                    ):
                        logger.info(f"Processing Excel file: {filename}")

                        file_path = os.path.join(dir_path, filename)
                        logger.info(f"Reading file: {file_path}")

                        parsed_current_file_generator = save_parse_excel(
                            file_path=Path(file_path)
                        )
                        # Ignore the header
                        next(parsed_current_file_generator)

                        for chunk_rows in batched(
                            iterable=parsed_current_file_generator,
                            n=COPY_INSERT_BATCH_NUM,
                        ):
                            chunk_len = len(chunk_rows)
                            start_time = time.time()
                            with pg_cursor.copy(
                                statement=landing_table_copy_query
                            ) as copy_cursor:
                                for row_number, row_data in enumerate(chunk_rows):
                                    if len(row_data) != len(table_columns_name):
                                        if LOG_INVALID_ROWS:
                                            logger.warning(
                                                "Row no. %d is not valid. Parsed data: %s",  # noqa: E501
                                                row_number + 1,
                                                row_data,
                                            )
                                        continue

                                    copy_cursor.write_row(row_data)

                            elapsed = time.time() - start_time

                            logger.info(
                                f"batch done: {round(chunk_len / elapsed, 3)} rows/s"
                            )

                    # CSV file (new type)
                    elif report_type == "new" and filename.endswith(".csv"):
                        logger.info(f"Processing CSV file: {filename}")
                        file_path = os.path.join(dir_path, filename)
                        logger.info(f"Reading file: {file_path}")

                        file_lines_generator = safe_readline_csv_like_file(
                            file_path=file_path,
                            encoding="utf-8",
                        )
                        parsed_current_file = csv.reader(
                            file_lines_generator,
                            delimiter=",",
                            quoting=csv.QUOTE_MINIMAL,
                        )

                        # Skip the header
                        next(parsed_current_file)

                        insert_parsed_file_to_landing_table(
                            pg_cursor=pg_cursor,
                            csv_reader=parsed_current_file,
                            table_name=table_name,
                            table_columns_name=table_columns_name,
                            row_validation_callback=lambda row: len(row)
                            == len(table_columns_name),
                        )

            except FileNotFoundError:
                logger.warning(f"File: {filename} is non-existent, ignoring...")
            except Exception as e:
                logger.error(f"Got error while reading {filename} file: {e}")
                raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset(
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=30,  # 30s delay from each retry
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    )
)
def id_fetch_old_idaho_campaign_finance():
    """
    Idaho campaign election finance report required by
    the Idaho Department of Elections.
    """
    id_fetch_all_files_of_all_report_type_all_time(
        base_url=ID_OLD_BASE_URL, report_type="old"
    )


@dg.asset(deps=[id_fetch_old_idaho_campaign_finance], pool="pg")
def id_insert_old_candidate_contributions_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert ID contributions data to landing table"""

    # Beware: this code is going to be a bit funky,
    # Check README.md for better understanding of why there's a ton of separate
    # inserts call.

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=ID_FIRST_YEAR_OF_OLD_DATA_AVAILABLE,
        last_year_inclusive=1994,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "office",
            "district",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=True,
        filter_file=lambda filename: filename.endswith("con.xls")
        and "pac" not in filename,
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=1996,
        last_year_inclusive=1996,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "legcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=1998,
        last_year_inclusive=1998,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "legcon.xls"
        or filename == "stwdcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=1998,
        last_year_inclusive=1998,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_first",
            "cand_middle",
            "cand_suffix",
            "office",
            "district",
            "party",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: "con" in filename
        and "comm" not in filename
        and filename != "legcon.xls"
        and filename != "stwdcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2000,
        last_year_inclusive=2000,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "office",
            "district",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "cand_contributions.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2001,
        last_year_inclusive=2001,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "office",
            "district",
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "cand_contributions.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2004,
        last_year_inclusive=2006,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "candcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2008,
        last_year_inclusive=2008,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_middle",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "candcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2010,
        last_year_inclusive=2010,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_suffix",
            "contrib_first",
            "contrib_middle",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "candcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2012,
        last_year_inclusive=2012,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_first",
            "cand_middle",
            "cand_suffix",
            "party",
            "district",
            "office",
            "contrib_type",
            "contrib_date",
            "contrib_amount",
            "contrib_cp",
            "contrib_committee_company",
            "contrib_first",
            "contrib_middle",
            "contrib_suffix",
            "contrib_line1",
            "contrib_line2",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
            "contrib_country",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename.endswith("_cand_cont.xlsx"),
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2014,
        last_year_inclusive=2016,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "cand_last",
            "cand_suffix",
            "cand_first",
            "cand_middle",
            "party",
            "district",
            "office",
            "contrib_type",
            "contrib_date",
            "contrib_amount",
            "contrib_cp",
            "contrib_committee_company",
            "contrib_first",
            "contrib_middle",
            "contrib_suffix",
            "contrib_line1",
            "contrib_line2",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
            "contrib_country",
            "election_type",
            "election_year",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename.endswith("_cand_cont.xlsx"),
    )

    return id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2018,
        last_year_inclusive=2018,
        table_name="id_old_candidate_contributions_landing",
        table_columns_name=[
            "party",
            "cand_last",
            "cand_first",
            "cand_middle",
            "office",
            "district",
            "contrib_date",
            "contrib_committee_company",
            "contrib_last",
            "contrib_first",
            "contrib_middle",
            "contrib_amount",
            "election_type",
            "contrib_type",
            "contrib_line1",
            "contrib_line2",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
            "contrib_country",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "candidate_contributions.xlsx",
    )


@dg.asset(deps=[id_fetch_old_idaho_campaign_finance], pool="pg")
def id_insert_old_committee_contributions_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert ID contributions data to landing table"""

    # Beware: this code is going to be a bit funky,
    # Check README.md for better understanding of why there's a ton of separate
    # inserts call.

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=ID_FIRST_YEAR_OF_OLD_DATA_AVAILABLE,
        last_year_inclusive=2006,
        table_name="id_old_committee_contributions_landing",
        table_columns_name=[
            "committee_name",
            "party",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=True,
        filter_file=lambda filename: filename.endswith("paccon.xls")
        or filename == "commcont.xls"
        or filename == "comm_contributions.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2008,
        last_year_inclusive=2008,
        table_name="id_old_committee_contributions_landing",
        table_columns_name=[
            "committee_name",
            "party",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_first",
            "contrib_middle",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "commcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2010,
        last_year_inclusive=2010,
        table_name="id_old_committee_contributions_landing",
        table_columns_name=[
            "committee_name",
            "party",
            "contrib_date",
            "contrib_amount",
            "contrib_type",
            "contrib_last",
            "contrib_suffix",
            "contrib_first",
            "contrib_middle",
            "contrib_line1",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "commcont.xls",
    )

    id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2012,
        last_year_inclusive=2016,
        table_name="id_old_committee_contributions_landing",
        table_columns_name=[
            "committee_name",
            "party",
            "contrib_type",
            "contrib_date",
            "contrib_amount",
            "contrib_cp",
            "contributor_name",
            "contrib_first",
            "contrib_middle",
            "contrib_suffix",
            "contrib_line1",
            "contrib_line2",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
            "contrib_country",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename.endswith("comm_cont.xlsx"),
    )

    return id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=2018,
        last_year_inclusive=2018,
        table_name="id_old_committee_contributions_landing",
        table_columns_name=[
            "committee_name",
            "party",
            "contrib_date",
            "contributor_name",
            "contrib_last",
            "contrib_first",
            "contrib_middle",
            "contrib_amount",
            "contrib_type",
            "contrib_line1",
            "contrib_line2",
            "contrib_city",
            "contrib_state",
            "contrib_zip",
            "contrib_country",
        ],
        report_type="old",
        truncate=False,
        filter_file=lambda filename: filename == "committee_contributions.xlsx",
    )


@dg.asset(
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=30,  # 30s delay from each retry
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    )
)
def id_fetch_new_idaho_campaign_finance():
    """
    Idaho campaign election finance report required by
    the Idaho Department of Elections.
    """
    id_fetch_all_files_of_all_report_type_all_time(
        base_url=ID_NEW_BASE_URL, report_type="new"
    )


@dg.asset(deps=[id_fetch_new_idaho_campaign_finance], pool="pg")
def id_insert_new_contributions_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert ID contributions data to landing table"""
    return id_insert_files_to_postgres(
        postgres_pool=pg.pool,
        start_year=ID_FIRST_YEAR_OF_NEW_DATA_AVAILABLE,
        last_year_inclusive=datetime.datetime.now().year,
        table_name="id_new_contributions_landing",
        table_columns_name=ID_NEW_CONTRIBUTION_COLUMNS,
        report_type="new",
    )

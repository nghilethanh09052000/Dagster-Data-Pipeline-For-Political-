import csv
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

ME_DATA_BASE_PATH = "./states/maine"

ME_MAIN_PAGE_URI = "https://mainecampaignfinance.com/"
ME_CSV_DOWNLOAD_API_URL = (
    "https://mainecampaignfinance.com/api///DataDownload/CSVDownloadReport"
)

ME_FIRST_YEAR_DATA_AVAILABLE = 2008
ME_FIRST_YEAR_NEW_FORMAT = 2018
ME_LAST_YEAR_OF_OLD_FORMAT = ME_FIRST_YEAR_NEW_FORMAT - 1

ME_MAIN_PAGE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.5",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "sec-gpc": "1",
    "upgrade-insecure-requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}
ME_FILE_DOWNLOAD_HEADERS = {
    "accept": "application/octet-stream",
    "accept-language": "en-US,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/json;charset=UTF-8",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua": '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "Referer": "https://mainecampaignfinance.com/",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}


def me_fetch_one_report_type(
    report_type: Literal["CON"] | Literal["EXP"],
    start_year: int,
):
    """
    Fetch all report data from start_year till current year. This function will
    stream the downloaded file to `./states/maine/{year}_{report_type}.csv`.

    Params:
    report_type: Maine data download report type, "CON" for contributions and loans,
                 and "EXP" for expenditures
    start_year: the first inclusive year you want the data to be downloaded
    """

    logger = dg.get_dagster_logger("me_fetch_" + report_type)

    base_dir = Path(ME_DATA_BASE_PATH)
    base_dir.mkdir(parents=True, exist_ok=True)

    last_year_inclusive = datetime.now().year

    for year in range(start_year, last_year_inclusive + 1):
        file_save_path = base_dir / f"{year}_{report_type}.csv"

        logger.info(
            f"Downloading report type {report_type} ({year}) to {file_save_path}"
        )

        with requests.session() as req_session:
            main_page_req = req_session.get(
                url=ME_MAIN_PAGE_URI, headers=ME_MAIN_PAGE_HEADERS
            )
            main_page_req.raise_for_status()

            with req_session.post(
                url=ME_CSV_DOWNLOAD_API_URL,
                json={"year": year, "transactionType": report_type},
                stream=True,
                headers=ME_FILE_DOWNLOAD_HEADERS,
            ) as file_response:
                if file_response.status_code >= 400 and file_response.status_code < 500:
                    logger.info(
                        f"Report type {report_type} ({year}) probably not found, "
                        f"status: {file_response.status_code}\n"
                        f"body: {file_response.text}"
                    )
                    continue

                file_response.raise_for_status()

                with open(file_save_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)


def me_insert_one_report_type_to_landing_table(
    pg_pool: ConnectionPool,
    report_type: Literal["CON"] | Literal["EXP"],
    landing_table_name: str,
    landing_table_columns: list[str],
    start_year: int,
    last_year_inclusive: int,
):
    """
    This function will insert the data from raw csv to the specified landing table.
    The function will get the raw files from the following format
    `./states/maine/{year}_{report_type}.csv`.

    Params:
    pg_pool: Postgres connection pool, could get it from Postgres resources
    report_type: Maine data download report type, "CON" for contributions and loans,
                 and "EXP" for expenditures
    landing_table_name: the name of the landing table the data to be inserted
    landing_table_columns: list of columns to be inserted, need to be sorted the same
                           way as the csv data schema
    start_year: the first inclusive year you want the data to be inserted
    last_year_inclusive: the last inclusive year you want the data to be inserted
    """

    logger = dg.get_dagster_logger("me_insert_" + report_type)
    truncate_query = get_sql_truncate_query(table_name=landing_table_name)

    base_dir = Path(ME_DATA_BASE_PATH)

    with (
        pg_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {landing_table_name} before inserting new data.")
        pg_cursor.execute(truncate_query)

        for year in range(start_year, last_year_inclusive + 1):
            raw_file_path = base_dir / f"{year}_{report_type}.csv"
            logger.info(f"Processing file: {raw_file_path}")

            if not raw_file_path.exists() or not raw_file_path.is_file():
                logger.warning(
                    f"File at either not exists or not a file: {raw_file_path}"
                )
                continue

            data_type_lines_generator = safe_readline_csv_like_file(
                raw_file_path, encoding="utf-8"
            )

            # Read as tab-separated values
            parsed_data_type_file = csv.reader(
                data_type_lines_generator, delimiter=",", quotechar='"'
            )

            # Skip the header
            next(parsed_data_type_file, None)

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_data_type_file,
                table_name=landing_table_name,
                table_columns_name=landing_table_columns,
                row_validation_callback=lambda row: len(row)
                == len(landing_table_columns),
            )

        # Verify row count
        count_query = get_sql_count_query(table_name=landing_table_name)

        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Successfully inserted {row_count} rows into {landing_table_name}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": landing_table_name,
                "dagster/row_count": row_count,
            }
        )


@dg.asset()
def me_contributions_and_loans():
    """
    This asset will download all contributions and loans report from Maine ethics
    commission from first year the data available until now.
    """

    me_fetch_one_report_type(report_type="CON", start_year=ME_FIRST_YEAR_DATA_AVAILABLE)


@dg.asset(deps=[me_contributions_and_loans])
def me_insert_old_format_contributions_and_loans(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    This asset will insert all the raw contributions and loans data on year 2008-2017
    """

    me_insert_one_report_type_to_landing_table(
        pg_pool=pg.pool,
        report_type="CON",
        landing_table_name="maine_old_contributions_and_loans_landing",
        landing_table_columns=[
            "OrgID",
            "ReceiptAmount",
            "ReceiptDate",
            "LastName",
            "FirstName",
            "MI",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "ReceiptID",
            "FiledDate",
            "ReceiptType",
            "ReceiptSourceType",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "Amended",
            "Description",
            "Employer",
            "Occupation",
            "OccupationComment",
            "EmploymentInfoRequested",
        ],
        start_year=ME_FIRST_YEAR_DATA_AVAILABLE,
        last_year_inclusive=ME_LAST_YEAR_OF_OLD_FORMAT,
    )


@dg.asset(deps=[me_contributions_and_loans])
def me_insert_new_format_contributions_and_loans(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    This asset will insert all the raw contributions and loans data on year 2017-now
    """

    current_year = datetime.now().year

    me_insert_one_report_type_to_landing_table(
        pg_pool=pg.pool,
        report_type="CON",
        landing_table_name="maine_new_contributions_and_loans_landing",
        landing_table_columns=[
            "OrgID",
            "LegacyID",
            "CommitteeName",
            "CandidateName",
            "ReceiptAmount",
            "ReceiptDate",
            "Office",
            "District",
            "LastName",
            "FirstName",
            "MiddleName",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Description",
            "ReceiptID",
            "FiledDate",
            "ReportName",
            "ReceiptSourceType",
            "ReceiptType",
            "CommitteeType",
            "Amended",
            "Employer",
            "Occupation",
            "OccupationComment",
            "EmploymentInformationRequested",
            "ForgivenLoan",
            "ElectionType",
        ],
        start_year=ME_FIRST_YEAR_NEW_FORMAT,
        last_year_inclusive=current_year,
    )


@dg.asset()
def me_expenditures():
    """
    This asset will download all expenditures report from Maine ethics
    commission from first year the data available until now.
    """

    me_fetch_one_report_type(report_type="EXP", start_year=ME_FIRST_YEAR_DATA_AVAILABLE)


@dg.asset(deps=[me_expenditures])
def me_insert_old_format_expenditures(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    This asset will insert all the raw expenditures data on year 2008-2017
    """

    me_insert_one_report_type_to_landing_table(
        pg_pool=pg.pool,
        report_type="EXP",
        landing_table_name="maine_old_expenditures_landing",
        landing_table_columns=[
            "OrgID",
            "ExpenditureAmount",
            "ExpenditureDate",
            "LastName",
            "FirstName",
            "MI",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Explanation",
            "ExpenditureID",
            "FiledDate",
            "Purpose",
            "ExpenditureType",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "Amended",
        ],
        start_year=ME_FIRST_YEAR_DATA_AVAILABLE,
        last_year_inclusive=ME_LAST_YEAR_OF_OLD_FORMAT,
    )


@dg.asset(deps=[me_expenditures])
def me_insert_new_format_expenditures(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    This asset will insert all the raw expenditures data on year 2017-now
    """

    current_year = datetime.now().year

    me_insert_one_report_type_to_landing_table(
        pg_pool=pg.pool,
        report_type="EXP",
        landing_table_name="maine_new_expenditures_landing",
        landing_table_columns=[
            "ElectionYear",
            "OrgID",
            "LegacyID",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "Jurisdiction",
            "Office",
            "District",
            "Party",
            "IncumbentStatus",
            "FinancingType",
            "PayeeLastName",
            "PayeeFirstName",
            "PayeeMiddleName",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "ExpenditureID",
            "ExpenditureDate",
            "ExpenditurePurpose",
            "ExpenditureAmount",
            "Explanation",
            "DateFiled",
            "Amended",
            "IEReport",
            "24HourReport",
            "ReportName",
            "OperatingExpense",
            "SupportOrOpposeBallotQuestion",
            "SupportOrOpposeCandidate",
            "BallotQuestionNumber",
            "BallotQuestionDescriptionOrTitle",
            "Candidate",
            "CandidateID",
            "Jurisdiction2",
            "CandidateOffice",
            "CandidateDistrict",
            "CandidateParty",
            "CandidateIncumbentStatus",
            "CandidateFinancingType",
        ],
        start_year=ME_FIRST_YEAR_NEW_FORMAT,
        last_year_inclusive=current_year,
    )

import csv
import logging
import typing
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_delete_by_year_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

VT_DATA_BASE_PATH = "./states/vermont"

VT_PROXIES: dict[str, str] = {
    "http": typing.cast(str, dg.EnvVar("VT_HTTP_PROXY").get_value(default="")),
    "https": typing.cast(str, dg.EnvVar("VT_HTTPS_PROXY").get_value(default="")),
}

VT_FIRST_YEAR_DATA_AVAILABLE = 2014
VT_FIRST_DATE_DATA_AVAILABLE = datetime(2014, 1, 1)

VT_MAIN_PAGE_URL = "https://campaignfinance.vermont.gov/public/cf/downloads"
VT_MAIN_PAGE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
    "image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.7",
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
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

VT_EXPORT_TCON_TEXP_CSV_URL = (
    "https://api.campaignfinance.vermont.gov/api/ExportData/GetExportPublicDownloadData"
)

VT_EXPORT_CSV_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua": '"Brave";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "sec-gpc": "1",
    "Referer": "https://campaignfinance.vermont.gov/",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

VT_REPORT_TYPE = ["TCON", "TEXP"]

VT_EXPORT_CAN_COM_CSV_URL = "https://api.campaignfinance.vermont.gov/api/PublicFilerDetails/DownloadPublicGridData"


def vt_fetch_data_tcon_texp_on_a_year(year: int):
    """
    This function will download all of the data available on Vermont campaign finance
    system download data system.

    The resulting file will be saved in the following format
    `./states/vermont/{year}/{report_type}.csv`

    Parameters:
    year: (int) the year that the data is going to be fetched
    """

    logger = logging.getLogger(vt_fetch_data_tcon_texp_on_a_year.__name__)
    base_dir = Path(VT_DATA_BASE_PATH)

    with requests.session() as req_session:
        main_page = req_session.get(
            url=VT_MAIN_PAGE_URL, proxies=VT_PROXIES, headers=VT_EXPORT_CSV_HEADERS
        )
        main_page.raise_for_status()

        year_base_dir = base_dir / str(year)
        year_base_dir.mkdir(parents=True, exist_ok=True)

        for report_type in VT_REPORT_TYPE:
            logger.info(f"Getting report type {report_type} ({year})")

            with req_session.post(
                url=VT_EXPORT_TCON_TEXP_CSV_URL,
                headers=VT_EXPORT_CSV_HEADERS,
                proxies=VT_PROXIES,
                json={
                    "transactionTypeCode": report_type,
                    "type": "CSV",
                    "filingYear": str(year),
                    "openInNewTab": False,
                },
                stream=True,
            ) as csv_file_req:
                if csv_file_req.status_code == 404 or csv_file_req.status_code == 400:
                    logger.warning(
                        f"Report Type {report_type} on year {year} non-exitent.\n"
                        f"Status Code: {csv_file_req.status_code}\n"
                        f"Body: {csv_file_req.text}"
                    )
                    continue

                csv_file_req.raise_for_status()

                file_save_path = year_base_dir / f"{report_type}.csv"
                with open(file_save_path, "wb") as output_file:
                    for chunk in csv_file_req.iter_content(chunk_size=8192):
                        output_file.write(chunk)


def vt_fetch_can_com_data(report_type: str, payload: dict[str, str]):
    """
    This function will download data of candiate and committee
    system download data system.

    The resulting file will be saved in the following format
    `./states/vermont/CAN.csv`
    `./states/vermont/COM.csv`

    Parameters:

    report_type: str the type candidate or committee
    payload: list[str, str] the payload of candidate and commitee

    """
    logger = logging.getLogger(vt_fetch_can_com_data.__name__)
    base_dir = Path(VT_DATA_BASE_PATH)
    base_dir.mkdir(parents=True, exist_ok=True)

    save_file_path = base_dir / f"{report_type}.csv"

    logger.info(f"Getting report type {report_type}")

    response = requests.post(
        url=VT_EXPORT_CAN_COM_CSV_URL,
        headers=VT_EXPORT_CSV_HEADERS,
        json=payload,
        proxies=VT_PROXIES,
        stream=True,
    )
    response.raise_for_status()
    logger.info(f"Saving {report_type} to {save_file_path}...")
    with open(save_file_path, "wb") as save_file:
        for chunk in response.iter_content(chunk_size=8192):
            save_file.write(chunk)


def vt_fetch_all_files_from_storage(
    base_url: str, file_names: list[str], start_year: int
):
    """
    This funciton is going to dowload all the files from a static download site.
    The expected URL format is "{base_url}/{year}/{either of file_names}".
    If there's 404 on any of the file_name, the function will only logs it,
    no error will be raised.

    Params:
    base_url: base url of the storage
    file_names: list of file names on the year that you want to download,
                e.g. ["TCON.csv", "TEXP.csv"]
    start_year: the first year you want to check for all of the file names in the
                base url, this will end in the current year and month
    """

    logger = dg.get_dagster_logger("vt_fetch_from_storage")
    base_path = Path(VT_DATA_BASE_PATH)

    last_year_inclusive = datetime.now().year

    for year in range(start_year, last_year_inclusive + 1):
        year_data_base_path = base_path / str(year)
        year_data_base_path.mkdir(parents=True, exist_ok=True)

        with requests.session() as req_session:
            for file_name in file_names:
                file_download_uri = f"{base_url}/{year}/{file_name}"

                logger.info(
                    f"Downloading {file_name} on year {year} ({file_download_uri})"
                )

                with req_session.get(file_download_uri, stream=True) as response:
                    if response.status_code == 404 or response.status_code == 400:
                        logger.warning(
                            f"File {file_name} on year {year} "
                            f"not found (URL: {file_download_uri}"
                        )
                        continue

                    response.raise_for_status()

                    file_save_path = year_data_base_path / file_name
                    logger.info(
                        f"Saving {file_name} on year {year} to {file_save_path}"
                    )

                    with open(file_save_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)


def vt_insert_all_files_of_one_data_type_to_landing_table(
    pg_pool: ConnectionPool,
    file_name: str,
    table_name: str,
    table_columns: list[str],
    year: int | None = None,
    delete_query: sql.Composed | None = None,
):
    """
    Insert all files of all year in file location of Vermont raw data.

    Params:
    pg_pool: the postgres pool from PostgresResource
    file_name: the name of the file based on the year folder, it should follow the
               template `./states/vermont/{year}/{file_name}`
    year: the year that you want to insert the data of
    table_name: landing table where the data is going to be inserted
    table_columns: list of columns in landing table sorted the same as the csv schema
    delete_query: the query used to delete existing rows of that year

    Supports two patterns:
    1️⃣ If `year` is given: ./vermont/{year}/{file_name}
    2️⃣ If `year` is None:  ./vermont/{file_name}

    """

    logger = dg.get_dagster_logger("vt_insert_" + file_name)
    base_path = Path(VT_DATA_BASE_PATH)

    with (
        pg_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(
            query=delete_query
            if delete_query
            else get_sql_truncate_query(table_name=table_name)
        )

        logger.info("Inserting data ...")
        current_path = (
            base_path / f"{year}/{file_name}" if year else base_path / file_name
        )

        if not current_path.exists():
            logger.warning(
                f"Path {current_path} is either not a file or doesn't"
                " exists, ignoring..."
            )
            return

        try:
            current_year_file_lines_generator = safe_readline_csv_like_file(
                file_path=current_path,
                encoding="utf-8",
            )
            parsed_current_year_file = csv.reader(
                current_year_file_lines_generator,
                delimiter=",",
                quoting=csv.QUOTE_MINIMAL,
            )

            # Skip the header
            next(parsed_current_year_file)

            def processed_row():
                for rows in parsed_current_year_file:
                    yield [*rows, str(year)] if year else rows

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=processed_row(),
                table_name=table_name,
                # Add special column to track partition_year
                table_columns_name=table_columns,
                # Add one columns to validation column count
                row_validation_callback=lambda row: len(row) == len(table_columns),
            )

        except FileNotFoundError:
            logger.warning(f"File for year {year} is non-existent, ignoring...")
        except Exception as e:
            logger.error(f"Got error while reading cycle year {year} file: {e}")
            raise e


vt_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=VT_FIRST_DATE_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(partitions_def=vt_yearly_partition)
def vt_fetch_campaign_data(context: dg.AssetExecutionContext):
    """
    Fetch all available campaign data from Supabase. Later on this function could
    use `vt_fetch_all_available_data` instead.
    """

    # NOTE: Use this if somehow you cannot scrape this anymore
    # vt_fetch_all_files_from_storage(
    #     base_url="https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/vt",
    #     file_names=[report_type + ".csv" for report_type in VT_REPORT_TYPE],
    #     start_year=VT_FIRST_YEAR_DATA_AVAILABLE,
    # )

    vt_fetch_data_tcon_texp_on_a_year(year=int(context.partition_key))


@dg.asset(partitions_def=vt_yearly_partition, deps=[vt_fetch_campaign_data])
def vt_insert_contributions_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all contributions raw data to landing table.
    """

    current_partition_year = int(context.partition_key)
    table_name = "vermont_contributions_landing"

    vt_insert_all_files_of_one_data_type_to_landing_table(
        pg_pool=pg.pool,
        file_name="TCON.csv",
        table_name=table_name,
        table_columns=[
            "FilingEntityId",
            "FilingEntityName",
            "CommitteeName",
            "RegistrationType",
            "TransactionId",
            "TransactionType",
            "TransactionSubtype",
            "GoodsDonatedOrServiceDonated",
            "ContributorType",
            "ContributorLastName",
            "ContributorFirstName",
            "ContributorAddressLine1",
            "ContributorAddressLine2",
            "ContributorAddressCity",
            "ContributorAddressState",
            "ContributorAddressZipCode",
            "TransactionDate",
            "TransactionAmount",
            "ElectionType",
            "ElectionYear",
            "TransactionComments",
            "Amended",
            "TimedReportName",
            "TimedReportFiledDate",
            "ReportName",
            "ReportFiledDate",
            "partition_year",
        ],
        year=current_partition_year,
        delete_query=get_sql_delete_by_year_query(
            table_name=table_name,
            column_name="partition_year",
            year=current_partition_year,
        ),
    )


@dg.asset(partitions_def=vt_yearly_partition, deps=[vt_fetch_campaign_data])
def vt_insert_expenditures_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all expenditures raw data to landing table.
    """

    current_partition_year = int(context.partition_key)
    table_name = "vermont_expenditures_landing"

    vt_insert_all_files_of_one_data_type_to_landing_table(
        pg_pool=pg.pool,
        file_name="TEXP.csv",
        table_name=table_name,
        table_columns=[
            "FilingEntityId",
            "FilingEntityName",
            "CommitteeName",
            "RegistrationType",
            "TransactionId",
            "TransactionType",
            "TransactionSubtype",
            "GoodsDonatedOrServiceDonated",
            "Purpose",
            "OtherPurposeComments",
            "PayeeType",
            "PayeeLastName",
            "PayeeFirstName",
            "PayeeAddressLine1",
            "PayeeAddressLine2",
            "PayeeAddressCity",
            "PayeeAddressState",
            "PayeeAddressZipCode",
            "TransactionDate",
            "TransactionAmount",
            "ElectionType",
            "ElectionYear",
            "TransactionComments",
            "Amended",
            "TimedReportName",
            "TimedReportFiledDate",
            "ReportName",
            "ReportFiledDate",
            "partition_year",
        ],
        year=current_partition_year,
        delete_query=get_sql_delete_by_year_query(
            table_name=table_name,
            column_name="partition_year",
            year=current_partition_year,
        ),
    )


@dg.asset()
def vt_fetch_candidates_data():
    payload = {
        "publicGridName": "CandidatePublicGrid",
        "candidateCommitteeSearchFilter": {
            "pageNumber": 1,
            "pageSize": 10,
            "filerTypeCode": "CAN",
            "filerName": "",
            "politicalPartyCode": "",
            "officeSought": "",
            "officeType": "",
            "town": "",
            "election": "",
            "electionYear": None,
            "filingYear": None,
            "totalRaisedMax": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "accountStatus": "FACT",
            "transactionSourceTypeCode": None,
            "treasurerName": None,
        },
        "fileName": "Candidates",
        "type": "CSV",
        "openInNewTab": False,
    }
    return vt_fetch_can_com_data(report_type="CAN", payload=payload)


@dg.asset(deps=[vt_fetch_candidates_data])
def vt_insert_candidates_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all expenditures raw data to landing table.
    """

    vt_insert_all_files_of_one_data_type_to_landing_table(
        pg_pool=pg.pool,
        file_name="CAN.csv",
        table_name="vermont_candidates_landing",
        table_columns=[
            "filing_entity_id",
            "candidate_last_name",
            "candidate_first_name",
            "candidate_middle_name",
            "candidate_address_line1",
            "candidate_address_line2",
            "candidate_city",
            "candidate_state",
            "candidate_zip_code",
            "candidate_email",
            "candidate_phone",
            "candidate_website",
            "office_type",
            "office",
            "town_or_city",
            "total_contributions",
            "total_expenditures",
            "election_cycle",
            "account_status",
            "treasurer_last_name",
            "treasurer_first_name",
            "treasurer_middle_name",
            "treasurer_email",
            "treasurer_phone",
            "treasurer_address_line1",
            "treasurer_address_line2",
            "treasurer_city",
            "treasurer_state",
            "treasurer_zip_code",
            "name_of_financial_institution",
            "name_of_financial_institution_address_line1",
            "name_of_financial_institution_address_line2",
            "name_of_financial_institution_city",
            "name_of_financial_institution_state",
            "name_of_financial_institution_zip_code",
        ],
    )


@dg.asset()
def vt_fetch_committee_data():
    payload = {
        "publicGridName": "CommitteePublicGrid",
        "candidateCommitteeSearchFilter": {
            "pageNumber": 1,
            "pageSize": 10,
            "filerTypeCode": "COM",
            "filerName": "",
            "committeeType": "",
            "treasurerName": "",
            "politicalPartyCode": "",
            "election": "",
            "totalRaisedMax": None,
            "filingYear": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "committeeSubType": "",
            "accountStatus": "FACT",
            "publicQuestion": "",
            "stance": "",
        },
        "fileName": "Committees",
        "type": "CSV",
        "openInNewTab": False,
    }
    return vt_fetch_can_com_data(report_type="COM", payload=payload)


@dg.asset(deps=[vt_fetch_committee_data])
def vt_insert_committees_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert all expenditures raw data to landing table.
    """

    vt_insert_all_files_of_one_data_type_to_landing_table(
        pg_pool=pg.pool,
        file_name="COM.csv",
        table_name="vermont_committees_landing",
        table_columns=[
            "filing_entity_id",
            "committee_name",
            "abbreviated_committee_name",
            "committee_type",
            "committee_sub_type",
            "committee_address",
            "committee_website",
            "treasurer_last_name",
            "treasurer_first_name",
            "financial_institution_name",
            "total_contributions",
            "total_expenditure",
            "election_cycle",
            "account_status",
            "town",
            "public_question",
            "stance",
        ],
    )

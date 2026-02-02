import csv
import glob
import os
import urllib.parse
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

TN_DATA_PATH_PREFIX = "./states/tennessee"
TN_BASE_URL = "https://apps.tn.gov"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Host": "apps.tn.gov",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": ('"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'),
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
}


def tn_get_year_options(html: str) -> list[str]:
    """
    Extracts the year options from the HTML response.

    Args:
        html (str): HTML content as a string.

    Returns:
        list[str]: List of year strings in descending order (latest year first).
    """
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", {"name": "yearSelection"})

    if not isinstance(select, Tag):
        return []

    options = select.find_all("option")
    years = []
    for opt in options:
        if isinstance(opt, Tag):
            value = opt.get("value")
            if isinstance(value, str) and value.isdigit():
                years.append(value)

    return sorted(years, reverse=True)


def tn_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
) -> dg.MaterializeResult:
    """
    Inserts raw CSV data from previously downloaded files into the specified
    PostgreSQL landing table.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL,
                                    typically injected from a Dagster resource.

        table_name (str): Name of the landing table to truncate and insert.

        table_columns_name (List[str]): List of column names in the
                                        correct order for insert.

        data_validation_callback (Callable): A function that ttnes a CSV row and returns
                                    a boolean indicating if the row should be inserted.

        category (str): The subdirectory under each year
                        (e.g., 'campaign_disclosure_form"'),
                        used to locate CSV files inside

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

    base_path = Path(TN_DATA_PATH_PREFIX)
    data_files = []

    glob_path = base_path / category

    if glob_path.exists():
        files = glob.glob(str(glob_path / f"*_{category}.csv"))
        data_files.extend(files)

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

                if os.path.getsize(data_file) == 0:
                    logger.warning(f"File {data_file} is empty, skipping...")
                    continue

                logger.info(f"Inserting from: {data_file}")

                # Open the CSV file and process it line by line
                data_type_lines_generator = safe_readline_csv_like_file(
                    data_file,
                    encoding="utf-8",
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=",", quotechar='"'
                )

                next(parsed_data_type_file)  # Skip header row

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
def tn_fetch_candidates_data():
    """
    Fetch candidate information from the Tennessee Secretary of State
    campaign finance portal.

    This function initiates a session, navigates to the candidate
    search page,
    and submits a POST request with a broad search payload to
    retrieve a comprehensive
    list of candidates along with their contact details,
    treasurer information, and party affiliation.

    The payload used in the POST request includes:
    - searchType: "both" (searches for both candidate dates and pac)

    - name, officeSelection, districtSelection, electionYearSelection,
        partySelection: blank (no filters)

    - nameField, contactField, treasurerNameField, treasurerContactField,
        partyField: "true" (includes all detailed fields)

    - _continue: "Continue" (submits the form)

    """
    logger = dg.get_dagster_logger("tn_fetch_candidates_data")

    name = "candidates"
    search_url = TN_BASE_URL + "/tncamp/public/cpsearch.htm"

    session = requests.Session()

    response = session.get(search_url, headers=HEADERS)

    logger.info("Fetching candidates data..")

    payload = {
        "searchType": "both",
        "name": "",
        "officeSelection": "",
        "districtSelection": "",
        "electionYearSelection": "",
        "partySelection": "",
        "nameField": "true",
        "contactField": "true",
        "treasurerNameField": "true",
        "treasurerContactField": "true",
        "partyField": "true",
        "_continue": "Continue",
    }

    response = session.post(search_url, headers=HEADERS, data=payload)

    logger.info(f"Session cookies: {session.cookies.get_dict()}")

    soup = BeautifulSoup(response.text, "html.parser")
    export_link = soup.find("a", string=lambda s: isinstance(s, str) and "CSV" in s)

    if isinstance(export_link, Tag) and export_link.get("href"):
        href = str(export_link.get("href", ""))
        full_csv_url = urllib.parse.urljoin(TN_BASE_URL, href)

        logger.info(f"CSV Export URL for : {full_csv_url}")

        base_dir = Path(TN_DATA_PATH_PREFIX).joinpath(name)

        base_dir.mkdir(parents=True, exist_ok=True)

        csv_file_path = f"{base_dir}/data_{name}.csv"

        cookie_header = "; ".join(
            [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
        )

        stream_download_file_to_path(
            request_url=full_csv_url,
            file_save_path=csv_file_path,
            headers={
                **HEADERS,
                "cookie": cookie_header,
            },
        )
        logger.info(f"Contributions Data saved to {csv_file_path}")
    else:
        logger.warning("No export link found!")


@dg.asset
def tn_fetch_contributions_data():
    """
    Fetches campaign contribution data from the Tennessee Secretary of
    State's campaign finance portal.

    This asset:
    - Initializes a session and sends a GET request to retrieve the base
        search page.

    - Parses the HTML to extract available reporting years.

    - Iterates over each year and submits a POST request with a
        payload specifying filter criteria.

    Payload fields:
    - `searchType`: Type of search to perform (e.g., "contributions").

    - `toType`: Indicates the recipient type; "both" includes all.

    - `fromCandidate`, `fromPAC`, `fromIndividual`, `fromOrganization`:
            Boolean flags to include contributions from each source type.

    - `yearSelection`: The reporting year for which contributions are being fetched.

    - Filtering fields (`recipientName`, `contributorName`, `employer`, etc.
             are left blank for a broad search.

    - `typeOf`, `amountSelection`, `amountDollars`, `amountCents`:
            Used to refine monetary filters (currently requesting all amounts).

    - Output field toggles (e.g., `typeField`, `amountField`, `dateField`, etc.):
        These control which columns appear in the search results.

    - `_continue`: Simulates clicking the "Continue" button to submit the form.

    """
    logger = dg.get_dagster_logger("tn_fetch_contributions_data")

    name = "contributions"
    search_url = TN_BASE_URL + "/tncamp/public/cesearch.htm"

    session = requests.Session()

    response = session.get(search_url, headers=HEADERS)

    html = response.text

    years = tn_get_year_options(html)

    for year in years:
        logger.info(f"Fetching contributions data for year: {year}")

        session.get(search_url, headers=HEADERS)

        payload = {
            "searchType": name,
            "toType": "both",
            "fromCandidate": "true",
            "fromPAC": "true",
            "fromIndividual": "true",
            "fromOrganization": "true",
            "electionYearSelection": "",
            "yearSelection": str(year),
            "recipientName": "",
            "contributorName": "",
            "employer": "",
            "occupation": "",
            "zipCode": "",
            "candName": "",
            "vendorName": "",
            "vendorZipCode": "",
            "purpose": "",
            "typeOf": "all",
            "amountSelection": "equal",
            "amountDollars": "",
            "amountCents": "",
            "typeField": "true",
            "adjustmentField": "true",
            "amountField": "true",
            "dateField": "true",
            "electionYearField": "true",
            "reportNameField": "true",
            "recipientNameField": "true",
            "contributorNameField": "true",
            "contributorAddressField": "true",
            "contributorOccupationField": "true",
            "contributorEmployerField": "true",
            "descriptionField": "true",
            "_continue": "Continue",
        }

        response = session.post(search_url, headers=HEADERS, data=payload)

        logger.info(f"Session cookies: {session.cookies.get_dict()}")

        soup = BeautifulSoup(response.text, "html.parser")
        export_link = soup.find("a", string=lambda s: isinstance(s, str) and "CSV" in s)

        if isinstance(export_link, Tag) and export_link.get("href"):
            href = str(export_link.get("href", ""))
            full_csv_url = urllib.parse.urljoin(TN_BASE_URL, href)

            logger.info(f"CSV Export URL for {year}: {full_csv_url}")

            base_dir = Path(TN_DATA_PATH_PREFIX).joinpath(name)

            base_dir.mkdir(parents=True, exist_ok=True)

            csv_file_path = f"{base_dir}/{year}_contributions.csv"

            cookie_header = "; ".join(
                [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
            )

            stream_download_file_to_path(
                request_url=full_csv_url,
                file_save_path=csv_file_path,
                headers={
                    **HEADERS,
                    "cookie": cookie_header,
                },
            )
            logger.info(f"Contributions Data saved to {csv_file_path}")
        else:
            logger.warning(f"No export link found for year {year}")


@dg.asset
def tn_fetch_expenditures_data():
    """
    Fetches campaign expenditures data from the Tennessee Secretary of
    State's campaign finance portal.

    This asset:
    - Initiates a session and loads the base search page to extract available
        reporting years.

    - Iterates through each year and submits a POST request to search for all
        expenditures made in that year.

    - Constructs a payload representing form fields typically submitted by
        the user via the website.

    Payload explanation:
    - `searchType`: Set to "expenditures" to specify the type of data.

    - `toType`: "both" to include all expenditure recipient types.

    - `toCandidate`, `toPac`, `toOther`: Boolean flags to include expenditures to
        candidates, PACs, and others.

    - `yearSelection`: The reporting year being queried (dynamically set in loop).

    - Filtering fields like `recipientName`, `vendorName`, `zipCode`, etc.,
        are left blank for a broad search.

    - `typeOf`, `amountSelection`, `amountDollars`, `amountCents`:
        These define monetary filter behavior (currently disabled to fetch all).

    - Output display fields (e.g., `typeField`, `amountField`, `dateField`, etc.):
        These determine which data columns appear in the search results.

    - `_continue`: Simulates pressing the "Continue" button on the search form.


    """
    logger = dg.get_dagster_logger("tn_fetch_expenditures_data")

    name = "expenditures"
    search_url = TN_BASE_URL + "/tncamp/public/cesearch.htm"

    session = requests.Session()

    response = session.get(search_url, headers=HEADERS)

    html = response.text

    years = tn_get_year_options(html)

    for year in years:
        logger.info(f"Fetching expenditures data for year: {year}")

        session.get(search_url, headers=HEADERS)

        payload = {
            "searchType": name,
            "toType": "both",
            "toCandidate": "true",
            "toPac": "true",
            "toOther": "true",
            "electionYearSelection": "",
            "yearSelection": str(year),
            "recipientName": "",
            "contributorName": "",
            "employer": "",
            "occupation": "",
            "zipCode": "",
            "candName": "",
            "vendorName": "",
            "vendorZipCode": "",
            "purpose": "",
            "typeOf": "all",
            "amountSelection": "equal",
            "amountDollars": "",
            "amountCents": "",
            "typeField": "true",
            "adjustmentField": "true",
            "amountField": "true",
            "dateField": "true",
            "electionYearField": "true",
            "reportNameField": "true",
            "candidatePACNameField": "true",
            "vendorNameField": "true",
            "vendorAddressField": "true",
            "purposeField": "true",
            "candidateForField": "true",
            "soField": "true",
            "_continue": "Continue",
        }

        response = session.post(search_url, headers=HEADERS, data=payload)

        logger.info(f"Session cookies: {session.cookies.get_dict()}")
        soup = BeautifulSoup(response.text, "html.parser")

        export_link = soup.find("a", string=lambda s: isinstance(s, str) and "CSV" in s)

        if isinstance(export_link, Tag) and export_link.get("href"):
            href = str(export_link.get("href", ""))
            full_csv_url = urllib.parse.urljoin(TN_BASE_URL, href)

            logger.info(f"CSV Export URL for {year}: {full_csv_url}")

            base_dir = Path(TN_DATA_PATH_PREFIX).joinpath(name)

            base_dir.mkdir(parents=True, exist_ok=True)

            csv_file_path = f"{base_dir}/{year}_expenditures.csv"

            cookie_header = "; ".join(
                [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
            )

            stream_download_file_to_path(
                request_url=full_csv_url,
                file_save_path=csv_file_path,
                headers={
                    **HEADERS,
                    "cookie": cookie_header,
                },
            )
            logger.info(f"Expenditures Data saved to {csv_file_path}")
        else:
            logger.warning(f"No export link found for year {year}")


@dg.asset(deps=[tn_fetch_candidates_data])
def tn_inserting_candidates_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Tennessee candidates CSVs into the PostgreSQL landing table.
    - Scans downloaded `*_candidates.csv` files inside the
        `candidate/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return tn_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="tn_candidates_landing",
        table_columns_name=[
            "salutation",
            "first_name",
            "last_name",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "phone",
            "email",
            "treasurer_name",
            "treasurer_address_1",
            "treasurer_address_2",
            "treasurer_city",
            "treasurer_state",
            "treasurer_zip",
            "treasurer_phone",
            "treasurer_email",
            "party_affiliation",
        ],
        data_validation_callback=lambda row: len(row) == 19,
        category="candidates",
    )


@dg.asset(deps=[tn_fetch_contributions_data])
def tn_inserting_contributions_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Tennessee contributions CSVs into the PostgreSQL landing table.
    - Scans downloaded `*_contributions.csv` files inside the
        `contributions/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return tn_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="tn_contributions_landing",
        table_columns_name=[
            "type",
            "adj",
            "amount",
            "date",
            "election_year",
            "report_name",
            "recipient_name",
            "contributor_name",
            "contributor_address",
            "contributor_occupation",
            "contributor_employer",
            "description",
        ],
        data_validation_callback=lambda row: len(row) == 12,
        category="contributions",
    )


@dg.asset(deps=[tn_fetch_expenditures_data])
def tn_inserting_expenditures_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Tennessee expenditures CSVs into the PostgreSQL landing table.
    - Scans downloaded `*_expenditures.csv` files inside the
        `expenditures/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return tn_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="tn_expenditures_landing",
        table_columns_name=[
            "type",
            "adj",
            "amount",
            "date",
            "election_year",
            "report_name",
            "candidate_pac_name",
            "vendor_name",
            "vendor_address",
            "purpose",
            "candidate_for",
            "s_o",
        ],
        data_validation_callback=lambda row: len(row) == 12,
        category="expenditures",
    )

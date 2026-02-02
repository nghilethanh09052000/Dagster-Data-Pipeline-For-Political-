import csv
import os
import shutil
from collections.abc import Callable
from datetime import datetime
from logging import Logger
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
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

OK_BASE_URL = "https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads"
OK_DATA_PATH_PREFIX = "./states/oklahoma"
OKLAHOMA_FIRST_YEAR_DATA_AVAILABLE = 2014
OK_COMMITTEE_SEARCH_URL = "https://guardian.ok.gov/PublicSite/SearchPages/Search.aspx?SearchTypeCodeHook=4E059E51-A3C3-45F5-A1BC-EA50C2AF9973"

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "referer": "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
    "sec-ch-ua": ('"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    ),
}

COMMITTEE_SEARCH_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://guardian.ok.gov",
    "referer": OK_COMMITTEE_SEARCH_URL,
    "sec-ch-ua": ('"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
}


def extract_committee_types(html_content: str) -> list[tuple[str, str]]:
    """Extract organization types from the select element.

    Args:
        html_content: HTML content containing the select element

    Returns:
        List of tuples containing (value, label) for each organization type
    """
    soup = BeautifulSoup(html_content, "html.parser")
    select = soup.find(
        "select",
        {
            "name": (
                "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$Committee_SearchManager$"
                "Committee_SearchParams$Manager_ConditionalName$OrganizationTypeCodeHook$ctl01"
            )
        },
    )

    if not isinstance(select, Tag):
        return []

    result: list[tuple[str, str]] = []
    for option in select.find_all("option"):
        if not isinstance(option, Tag):
            continue

        value = option.attrs.get("value")
        if not isinstance(value, str):
            continue

        text = option.get_text(strip=True)
        if not isinstance(text, str):
            continue

        if value and text:
            result.append((value, text))

    return result


def download_and_extract_csv(url: str, output_dir: str, logger: Logger) -> None:
    """
    Downloads a ZIP file from the given URL, extracts the contained CSV file,
    and saves it to the specified output directory.

    Example of download urls format:

    - https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2025_ContributionLoanExtract.csv.zip

    - https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2025_ExpenditureExtract.csv.zip

    - https://guardian.ok.gov/PublicSite/Docs/BulkDataDownloads/2025_LobbyistExpenditures.csv.zip

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

    stream_download_file_to_path(url, zip_path, headers=HEADERS)

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


def build_committee_search_payload(html_content: str, committee_type: str) -> dict:
    """Build payload for committee search request.

    Args:
        html_content: HTML content containing form state
        committee_type: The committee type value

    Returns:
        Dictionary containing form payload
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract hidden form fields
    viewstate = soup.find("input", {"name": "__VIEWSTATE"})
    viewstategenerator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
    eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})

    # Get values with type checking
    viewstate_value = viewstate.get("value", "") if isinstance(viewstate, Tag) else ""
    viewstategenerator_value = (
        viewstategenerator.get("value", "")
        if isinstance(viewstategenerator, Tag)
        else ""
    )
    eventvalidation_value = (
        eventvalidation.get("value", "") if isinstance(eventvalidation, Tag) else ""
    )

    return {
        "__EVENTTARGET": (
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Committee_SearchButton$ctl01"
        ),
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate_value,
        "__VIEWSTATEGENERATOR": viewstategenerator_value,
        "__EVENTVALIDATION": eventvalidation_value,
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$OrganizationTypeCodeHook$ctl01": committee_type,
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$Manager_PACTypeFilter$PACType$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$PACState$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$LastName$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$NameTextSearchTypeCodeHook$ctl01": (
            "9730ABEC-5F6B-4DCF-916B-8398A06484C6"
        ),
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$Acronym$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$AcronymTextSearchTypeCodeHook$ctl01": (
            "9730ABEC-5F6B-4DCF-916B-8398A06484C6"
        ),
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "OrganizationStatusCodeID$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$ElectionID$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$DateFrom$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$DateThrough$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$SubReportType$ctl01": "1",
    }


def build_export_payload(
    html_content: str, committee_type: str, committee_name: str
) -> dict:
    """Build payload for exporting committee data.

    Args:
        html_content: HTML content containing form state
        committee_type: The committee type value
        committee_name: The committee type name (for determining export format)

    Returns:
        Dictionary containing export form payload
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract hidden form fields
    viewstate = soup.find("input", {"name": "__VIEWSTATE"})
    viewstategenerator = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
    eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})

    # Get values with type checking
    viewstate_value = viewstate.get("value", "") if isinstance(viewstate, Tag) else ""
    viewstategenerator_value = (
        viewstategenerator.get("value", "")
        if isinstance(viewstategenerator, Tag)
        else ""
    )
    eventvalidation_value = (
        eventvalidation.get("value", "") if isinstance(eventvalidation, Tag) else ""
    )

    # Common fields for all committee types
    common_fields = {
        "ctl00_ToolkitScriptManager1_HiddenField": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate_value,
        "__VIEWSTATEGENERATOR": viewstategenerator_value,
        "__SCROLLPOSITIONX": "0",
        "__SCROLLPOSITIONY": "254",
        "__EVENTVALIDATION": eventvalidation_value,
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$OrganizationTypeCodeHook$ctl01": committee_type,
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$LastName$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$NameTextSearchTypeCodeHook$ctl01": (
            "9730ABEC-5F6B-4DCF-916B-8398A06484C6"
        ),
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$Acronym$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "Manager_ConditionalName$AcronymTextSearchTypeCodeHook$ctl01": (
            "9730ABEC-5F6B-4DCF-916B-8398A06484C6"
        ),
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$"
        "OrganizationStatusCodeID$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$ElectionID$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$DateFrom$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$DateThrough$ctl01": "",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Committee_SearchParams$SubReportType$ctl01": "1",
        "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
        "Committee_SearchManager$Search_Export$mgrReportViewer$"
        "Search_Output$ctl01$ctl13$Search_OutputPageSizeDropDown": "10",
    }

    # Committee-specific fields
    committee_specific_fields = {
        "Political Action Committee": {
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Committee_SearchParams$"
            "Manager_ConditionalName$Manager_PACTypeFilter$PACType$ctl01": "",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Committee_SearchParams$"
            "Manager_ConditionalName$PACState$ctl01": "",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.x": "11",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.y": "15",
        },
        "Political Party Committee": {
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Committee_SearchParams$"
            "Manager_ConditionalName$PartyType$ctl01": "",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.x": "12",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.y": "12",
        },
        "Special Function Committee": {
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.x": "17",
            "ctl00$Content$4e059e51-a3c3-45f5-a1bc-ea50c2af9973$"
            "Committee_SearchManager$Search_Export$ctl03.y": "18",
        },
    }

    # Start with common fields
    payload = common_fields.copy()

    # Add committee-specific fields if they exist
    if committee_name in committee_specific_fields:
        payload.update(committee_specific_fields[committee_name])

    return payload


def ok_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    category: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Inserts Oklahoma raw CSV data from previously downloaded files into the specified
    PostgreSQL landing table.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL,
                                    typically injected from a Dagster resource.

        category (str): The subdirectory under each year
                        (e.g., 'ContributionLoanExtract'),
                        used to locate CSV files inside
                        `states/oklahoma/{year}/{category}`.

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
    base_path = Path(OK_DATA_PATH_PREFIX)
    data_files = []

    for year in range(2014, current_year + 1):
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
def ok_contributions_loans_download_data() -> None:
    """
    Downloads and extracts Oklahoma contributions and loans CSV data.
    This asset fetches the data from a URL, extracts the CSV files, and saves them
    to the specified output directory.
    """
    logger = dg.get_dagster_logger(name="ok_contributions_loans_download_data")
    current_year = datetime.now().year
    for year in range(OKLAHOMA_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        url = f"{OK_BASE_URL}/{year}_ContributionLoanExtract.csv.zip"
        output_dir = f"states/oklahoma/{year}/ContributionLoanExtract"
        download_and_extract_csv(url, output_dir, logger)


@dg.asset
def ok_expenditures_download_data() -> None:
    """
    Downloads and extracts Oklahoma expenditures CSV data.
    This asset fetches the data from a URL, extracts the CSV files, and saves them
    to the specified output directory.
    """
    logger = dg.get_dagster_logger(name="ok_expenditures_download_data")
    current_year = datetime.now().year
    for year in range(OKLAHOMA_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        url = f"{OK_BASE_URL}/{year}_ExpenditureExtract.csv.zip"
        output_dir = f"states/oklahoma/{year}/ExpenditureExtract"
        download_and_extract_csv(url, output_dir, logger)


@dg.asset
def ok_lobbyist_expenditures_download_data() -> None:
    """
    Downloads and extracts Oklahoma Lobbyist Expenditures CSV data.
    This asset fetches the data from a URL, extracts the CSV files, and saves them
    to the specified output directory.
    """
    logger = dg.get_dagster_logger(name="ok_lobbyist_expenditures_download_data")
    current_year = datetime.now().year
    for year in range(OKLAHOMA_FIRST_YEAR_DATA_AVAILABLE, current_year + 1):
        url = f"{OK_BASE_URL}/{year}_LobbyistExpenditures.csv.zip"
        output_dir = f"states/oklahoma/{year}/LobbyistExpenditures"
        download_and_extract_csv(url, output_dir, logger)


@dg.asset
def ok_committees_download_data() -> None:
    """Downloads Oklahoma committee data from the Ethics Commission website.

    This asset performs the following steps:
    1. Loads the initial committee search form
    2. Submits the search form to get results
    3. Exports the results to CSV
    4. Saves the CSV file to the output directory
    """
    import requests

    logger = dg.get_dagster_logger(name="ok_committees_download_data")
    session = requests.Session()

    # Step 1: Get initial page
    logger.info("Loading initial committee search page...")
    initial_response = session.get(
        OK_COMMITTEE_SEARCH_URL, headers=COMMITTEE_SEARCH_HEADERS
    )
    initial_response.raise_for_status()

    commitee_types = extract_committee_types(initial_response.text)
    for committee_type, name in commitee_types:
        # Step 2: Submit search form
        logger.info("Submitting committee search form...")
        payload = build_committee_search_payload(initial_response.text, committee_type)

        second_response = session.post(
            OK_COMMITTEE_SEARCH_URL,
            data=payload,
            headers=COMMITTEE_SEARCH_HEADERS,
        )
        second_response.raise_for_status()

        logger.info("Exporting committee data...")
        export_payload = build_export_payload(
            second_response.text, committee_type, name
        )
        third_response = session.post(
            OK_COMMITTEE_SEARCH_URL,
            data=export_payload,
            headers=COMMITTEE_SEARCH_HEADERS,
        )
        third_response.raise_for_status()

        # Step 4: Save CSV
        current_year = datetime.now().year
        output_dir = Path(OK_DATA_PATH_PREFIX) / f"{current_year}" / "committees"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{name}_committees.csv"

        with open(output_file, "wb") as f:
            f.write(third_response.content)

        logger.info(f"Saved committee data to {output_file}")


@dg.asset(deps=[ok_contributions_loans_download_data])
def ok_insert_contributions_loans(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts Oklahoma contributions and loans data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ok_contributions_loans_landing' table
    """
    return ok_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="ContributionLoanExtract",
        table_name="ok_contributions_loans_landing",
        table_columns_name=[
            "receipt_id",
            "org_id",
            "receipt_type",
            "receipt_date",
            "receipt_amount",
            "description",
            "receipt_source_type",
            "last_name",
            "first_name",
            "middle_name",
            "suffix",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "filed_date",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
            "employer",
            "occupation",
        ],
        data_validation_callback=lambda row: len(row) == 23,
    )


@dg.asset(deps=[ok_expenditures_download_data])
def ok_insert_expenditures(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts Oklahoma expenditure data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ok_expenditures_landing' table
    """
    return ok_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="ExpenditureExtract",
        table_name="ok_expenditures_landing",
        table_columns_name=[
            "expenditure_id",
            "org_id",
            "expenditure_type",
            "expenditure_date",
            "expenditure_amount",
            "description",
            "purpose",
            "last_name",
            "first_name",
            "middle_name",
            "suffix",
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "filed_date",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
            "employer",
            "occupation",
        ],
        data_validation_callback=lambda row: len(row) == 23,
    )


@dg.asset(deps=[ok_lobbyist_expenditures_download_data])
def ok_insert_lobbyist_expenditures(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts Oklahoma lobbyist expenditure data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ok_lobbyist_expenditures_landing' table
    """
    return ok_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="LobbyistExpenditures",
        table_name="ok_lobbyist_expenditures_landing",
        table_columns_name=[
            "expenditure_id",
            "lobbyist_id",
            "lobbyist_first_name",
            "lobbyist_middle_name",
            "lobbyist_last_name",
            "lobbyist_suffix",
            "expenditure_type",
            "expenditure_date",
            "expenditure_cost",
            "meal_type",
            "other_meal_description",
            "explanation",
            "recipient_first_name",
            "recipient_middle_name",
            "recipient_last_name",
            "recipient_suffix",
            "recipient_type",
            "recipient_title",
            "recipient_agency_office",
            "relationship_to_state_officer_or_employee",
            "family_member_name",
            "principal_name",
            "principals_percentage_cost",
            "caucus",
            "committee_subcommittee",
            "event_location",
            "event_city",
            "event_state",
        ],
        data_validation_callback=lambda row: len(row) == 28,
    )


@dg.asset(deps=[ok_committees_download_data])
def ok_insert_committees(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserts Oklahoma lobbyist expenditure data into the landing table.

    This asset processes the downloaded data and inserts it into the
    'ok_committees_landing' table
    """
    return ok_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="committees",
        table_name="ok_committees_landing",
        table_columns_name=[
            "name",
            "acronym",
            "type",
            "status",
            "email",
            "report",
            "filed",
            "has_amendment",
            "empty_column",
        ],
        data_validation_callback=lambda row: len(row) == 9,
    )

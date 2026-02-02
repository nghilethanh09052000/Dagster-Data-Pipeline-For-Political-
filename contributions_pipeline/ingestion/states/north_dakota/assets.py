import csv
import glob
import os
import re
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants for North Dakota
ND_DATA_PATH_PREFIX = "./states/north_dakota"
ND_SEARCH_URL = "https://cf.sos.nd.gov/search/cfsearch.aspx"

# Headers for requests
HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
}

# Headers for POST requests
POST_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://cf.sos.nd.gov",
    "referer": ND_SEARCH_URL,
    "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
    "x-microsoftajax": "Delta=true",
    "x-requested-with": "XMLHttpRequest",
}

# Define column headers for North Dakota candidates
ND_CANDIDATE_COLUMNS = [
    "candidate_name",
    "committee_name",
    "street_city",
    "state",
    "zip",
    "type",
    "office",
    "district",
    "year",
]

# Define column headers for North Dakota committees
ND_COMMITTEE_COLUMNS = [
    "year",
    "committee_name",
    "street",
    "city",
    "state",
    "zip",
    "phone",
    "email",
    "committee_type",
    "empty_column",
]

# Define column headers for North Dakota contributions
ND_CONTRIBUTION_COLUMNS = [
    "contributor",
    "street",
    "city",
    "state",
    "zip",
    "date",
    "amount",
    "contributed_to",
    "empty_column",
]

# Define column headers for North Dakota contributions
ND_EXPENDITURE_COLUMNS = [
    "expenditure_made_by",
    "street",
    "city",
    "state",
    "zip",
    "date",
    "amount",
    "recipient",
    "empty_column",
]


def nm_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
) -> dg.MaterializeResult:
    """Insert raw CSV data from previously downloaded files into the
    specified PostgreSQL landing table.

    This function processes CSV files from a specific category directory,
    validates each row,
    and inserts the data into the corresponding landing table in PostgreSQL.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL.
        table_name (str): Name of the landing table to insert data into.
        table_columns_name (list[str]): List of column names in the correct
        order for insert.
        data_validation_callback (Callable[[list[str]], bool]): A function
        that validates each row
            before insertion. Returns True if the row should be inserted.
        category (str): The subdirectory under each year
        (e.g., 'candidates', 'committees').

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing:
            - The table name
            - The number of rows inserted

    Raises:
        Exception: If there's an error during file processing or database operations.
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    base_path = Path(ND_DATA_PATH_PREFIX)
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

                data_type_lines_generator = safe_readline_csv_like_file(
                    data_file, encoding="utf-8"
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=","
                )

                next(parsed_data_type_file)

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


def nd_extract_hidden_input_value(html_content, key):
    """Extract hidden input value from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    element = soup.find("input", {"type": "hidden", "name": key})

    if isinstance(element, Tag) and element.has_attr("value"):
        return element["value"]

    if key == "__VIEWSTATE":
        match = re.search(r"__VIEWSTATE\|(.*?)\|8\|hiddenField\|", html_content)
        return match.group(1) if match else ""
    elif key == "__VIEWSTATEGENERATOR":
        match = re.search(r"__VIEWSTATEGENERATOR\|([A-Za-z0-9]+)\|", html_content)
        return match.group(1) if match else ""
    elif key == "__EVENTVALIDATION":
        match = re.search(r"__EVENTVALIDATION\|([A-Za-z0-9+/=]+)\|", html_content)
        return match.group(1) if match else ""
    return ""


def nd_extract_radscriptmanager_script(html_content: str) -> str:
    """
    Extracts the Telerik RadScriptManager script tag from the provided HTML content.

    Parameters:
    - html_content (str): The HTML content of the page to search within.

    Returns:
    - str: The extracted script tag if found, else an empty string.
    """
    # Regular expression pattern to find the RadScriptManager script tag
    script_pattern = re.compile(
        r'<script\s+src="(/Telerik.Web.UI.WebResource\.axd\?[^"]+)"\s+type="text/javascript"></script>'
    )

    match = script_pattern.search(html_content)
    if match:
        return match.group(1)
    return ""


def clean_candidate_data(raw_csv_data) -> list[str]:
    """Clean and format candidate data row.

    Args:
        row (list[str]): Raw row data from CSV

    Returns:
        list[str]: Cleaned and formatted row data with proper column structure
    """
    # Clean each field by stripping whitespace
    """
    Cleans the raw CSV data and writes it to a new CSV file.

    Parameters:
        raw_csv_data (str): The raw CSV data as a string.
        csv_file_path (str): The path to save the cleaned CSV file.

    Returns:
        None
    """
    # Split the raw CSV data into lines
    import io

    cleaned_data = []
    reader = csv.reader(io.StringIO(raw_csv_data))

    for row in reader:
        if not row:
            continue

        # Header row
        if "Candidate Name" in row[0]:
            cleaned_data.append(
                [
                    "Candidate Name",
                    "Committee Name",
                    "Street + City",
                    "State",
                    "Zip",
                    "Type",
                    "Office",
                    "District",
                    "Year",
                ]
            )
            continue

        if len(row) < 10:
            continue

        # Merge Candidate Name
        candidate_name = f"{row[0].strip()}, {row[1].strip()}"

        # Committee Name
        committee_name = row[2].strip()

        # Merge Street + City
        address = f"{row[3].strip()} {row[4].strip()}"

        # State, Zip, Type, Office, District, Year
        state = row[6].strip()
        zip_code = row[7].strip()
        type_ = row[8].strip()
        office = row[9].strip()
        district = row[10].strip()
        year = row[11].strip()

        cleaned_data.append(
            [
                candidate_name,
                committee_name,
                address,
                state,
                zip_code,
                type_,
                office,
                district,
                year,
            ]
        )

    return cleaned_data


# ==================
# North Dakota Candidate and Committee Data Ingestion
# ==================
@dg.asset
def nd_get_initialize_search_session() -> tuple[requests.Session, str]:
    """Initialize search session

    This function:
    1. Makes initial request to get cookies
    3. Returns the session and first response text

    Returns:
        tuple[requests.Session, str]: A tuple containing:
            - The initialized session with cookies
            - The first response text containing viewstate and other required values
    """
    logger = dg.get_dagster_logger("nd_get_initialize_search_session")

    session = requests.Session()
    session.headers.update(HEADERS)

    # Step 1: Initial GET request to get cookies
    logger.info("Making initial GET request...")
    initial_response = session.get(ND_SEARCH_URL)
    initial_response.raise_for_status()

    return session, initial_response.text


@dg.asset(deps=[nd_get_initialize_search_session])
def nd_fetch_candidates_data(
    nd_get_initialize_search_session: tuple[requests.Session, str],
) -> None:
    """Fetch North Dakota candidate data by submitting search form.

    This function depends on nd_get_initialize_search_session and:
    1. Selects candidate search
    2. For each letter A-Z:
       - Makes search request
       - Exports to CSV
       - Cleans and saves the data
    """
    logger = dg.get_dagster_logger("nd_fetch_candidates_data")
    session, initial_response = nd_get_initialize_search_session

    # Create output directory if it doesn't exist
    output_dir = Path(ND_DATA_PATH_PREFIX) / "candidates"
    output_dir.mkdir(parents=True, exist_ok=True)

    current_year = datetime.now().year

    for year in range(2014, current_year + 1):
        logger.info(f"Making first POST request to select for the year {year}")

        year_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$rcbxOnlyYear"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "__EVENTTARGET": "ctl00$MainContent$rcbxOnlyYear",
            "__EVENTARGUMENT": '{"Command":"Select","Index":1}',
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                initial_response, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": (
                f"{{"
                f'"logEntries":[],'
                f'"value":"{year}",'
                f'"text":"{year}",'
                f'"enabled":true,'
                f'"checkedIndices":[],'
                f'"checkedItemsTextOverflows":false'
                f"}}"
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":false'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        year_response = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=year_payload
        )
        year_response.raise_for_status()
        year_response_text = year_response.text

        # Select candidate search
        logger.info("Making second POST request to select candidate search...")
        candidate_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$radio3"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                year_response_text
            ),
            "__EVENTTARGET": "ctl00$MainContent$radio3",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                year_response_text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                year_response_text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                year_response_text, "__EVENTVALIDATION"
            ),
            "ctl00$txtSearch": "",
            "ctl00$MainContent$rcbxOnlyYear": "All",
            "ctl00$MainContent$searchCriteria": "radio3",
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        candidate_search = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=candidate_payload
        )
        candidate_search.raise_for_status()

        # Base payload for final search
        base_payload = {
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                year_response_text
            ),
            "__EVENTTARGET": "ctl00$MainContent$searchButton",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                candidate_search.text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                candidate_search.text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                candidate_search.text, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00$MainContent$ScriptManager1": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": "All",
            "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
            "ctl00$MainContent$searchCriteria": "radio3",
            "ctl00$MainContent$officetypeComboBox": "All",
            "ctl00_MainContent_officetypeComboBox_ClientState": "",
            "ctl00$MainContent$officeComboBox": "All",
            "ctl00_MainContent_officeComboBox_ClientState": "",
            "ctl00_MainContent_districtComboBox_ClientState": (
                '{"logEntries":null,"value":"","text":"","enabled":false,'
                '"checkedIndices":[],"checkedItemsTextOverflows":false}'
            ),
            "ctl00_MainContent_mngBtnCSV_ClientState": (
                '{"text":"Export Excel","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                '{"text":"Search","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                '{"text":"Clear","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                '{"text":"Open New Window Search","value":"","checked":false,'
                '"target":"","navigateUrl":"","commandName":"",'
                '"commandArgument":"","autoPostBack":false,'
                '"selectedToggleStateIndex":0,"validationGroup":null,'
                '"readOnly":false,"primary":false,"enabled":true}'
            ),
            "ctl00$MainContent$RadGrid3$ctl00$ctl03$ctl01$GoToPageTextBox": "1",
            "ctl00_MainContent_RadGrid3_ctl00_ctl03_ctl01_"
            "GoToPageTextBox_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"1",'
                '"valueAsString":"1","minValue":1,"maxValue":88,'
                '"lastSetTextBoxValue":"1"}'
            ),
            "ctl00$MainContent$RadGrid3$ctl00$ctl03$ctl01$ChangePageSizeTextBox": "10",
            "ctl00_MainContent_RadGrid3_ctl00_ctl03_ctl01_"
            "ChangePageSizeTextBox_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"10",'
                '"valueAsString":"10","minValue":1,"maxValue":878,'
                '"lastSetTextBoxValue":"10"}'
            ),
            "ctl00_MainContent_RadGrid3_ClientState": "",
        }

        # Process each letter A-Z
        logger.info("Starting A-Z search loop...")
        for letter in "abcdefghijklmnopqrstuvwxyz":
            logger.info(f"Processing candidates starting with '{letter}'...")

            # Update payload with current letter
            payload = base_payload.copy()
            payload["ctl00$MainContent$candnameBox"] = letter

            # Make the search request
            final_response = session.post(ND_SEARCH_URL, headers=HEADERS, data=payload)
            final_response.raise_for_status()

            # Make CSV export request
            logger.info(f"Exporting CSV for candidates starting with '{letter}'...")
            export_payload = payload.copy()
            export_payload["__EVENTTARGET"] = "ctl00$MainContent$mngBtnCSV"
            export_payload["__VIEWSTATE"] = nd_extract_hidden_input_value(
                final_response.text, "__VIEWSTATE"
            )
            export_payload["__VIEWSTATEGENERATOR"] = nd_extract_hidden_input_value(
                final_response.text, "__VIEWSTATEGENERATOR"
            )
            export_payload["__EVENTVALIDATION"] = nd_extract_hidden_input_value(
                final_response.text, "__EVENTVALIDATION"
            )

            export_response = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=export_payload
            )
            export_response.raise_for_status()

            csv_file = output_dir / f"{year}_{letter}_candidates.csv"

            data = clean_candidate_data(export_response.text)

            with open(csv_file, "w", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(data)

            logger.info(
                f"Candidates Csv File Downloaded For Year {year}\
                    With Letter {letter} On Path {csv_file}"
            )


@dg.asset(deps=[nd_get_initialize_search_session])
def nd_fetch_committees_data(
    nd_get_initialize_search_session: tuple[requests.Session, str],
) -> None:
    """Fetch committee information from the North Dakota SOS portal.

    This function depends on nd_get_initialize_search_session and:
    1. Selects committee search
    2. For each letter A-Z:
       - Makes search request
       - Exports to CSV
       - Cleans and saves the data
    """
    logger = dg.get_dagster_logger("nd_fetch_committees_data")
    session, initial_response = nd_get_initialize_search_session

    # Create directory for storing committee data
    base_dir = Path(ND_DATA_PATH_PREFIX) / "committees"
    base_dir.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now().year

    for year in range(2014, current_year + 1):
        logger.info(f"Making first POST request to select for the year {year}")

        year_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$rcbxOnlyYear"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "__EVENTTARGET": "ctl00$MainContent$rcbxOnlyYear",
            "__EVENTARGUMENT": '{"Command":"Select","Index":1}',
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                initial_response, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": (
                f"{{"
                f'"logEntries":[],'
                f'"value":"{year}",'
                f'"text":"{year}",'
                f'"enabled":true,'
                f'"checkedIndices":[],'
                f'"checkedItemsTextOverflows":false'
                f"}}"
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":false'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        year_response = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=year_payload
        )
        year_response.raise_for_status()
        year_response_text = year_response.text

        # Select committee search
        logger.info("Making second POST request to select committee search...")
        committee_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$radio4"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                year_response_text
            ),
            "__EVENTTARGET": "ctl00$MainContent$radio4",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                year_response_text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                year_response_text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                year_response_text, "__EVENTVALIDATION"
            ),
            "ctl00$txtSearch": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00$MainContent$searchCriteria": "radio4",
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        committee_search = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=committee_payload
        )
        committee_search.raise_for_status()

        # Base payload for final search
        base_payload = {
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                year_response_text
            ),
            "__EVENTTARGET": "ctl00$MainContent$searchButton",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                committee_search.text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                committee_search.text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                committee_search.text, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00$MainContent$ScriptManager1": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
            "ctl00$MainContent$searchCriteria": "radio4",
            "ctl00$MainContent$officetypeComboBox": "All",
            "ctl00_MainContent_officetypeComboBox_ClientState": "",
            "ctl00$MainContent$officeComboBox": "All",
            "ctl00_MainContent_officeComboBox_ClientState": "",
            "ctl00_MainContent_districtComboBox_ClientState": (
                '{"logEntries":null,"value":"","text":"","enabled":false,'
                '"checkedIndices":[],"checkedItemsTextOverflows":false}'
            ),
            "ctl00_MainContent_mngBtnCSV_ClientState": (
                '{"text":"Export Excel","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                '{"text":"Search","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                '{"text":"Clear","value":"","checked":false,"target":"",'
                '"navigateUrl":"","commandName":"","commandArgument":"",'
                '"autoPostBack":true,"selectedToggleStateIndex":0,'
                '"validationGroup":null,"readOnly":false,"primary":false,'
                '"enabled":true}'
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                '{"text":"Open New Window Search","value":"","checked":false,'
                '"target":"","navigateUrl":"","commandName":"",'
                '"commandArgument":"","autoPostBack":false,'
                '"selectedToggleStateIndex":0,"validationGroup":null,'
                '"readOnly":false,"primary":false,"enabled":true}'
            ),
            "ctl00$MainContent$RadGrid3$ctl00$ctl03$ctl01$GoToPageTextBox": "1",
            "ctl00_MainContent_RadGrid3_ctl00_ctl03_"
            "ctl01_GoToPageTextBox_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"1",'
                '"valueAsString":"1","minValue":1,"maxValue":88,'
                '"lastSetTextBoxValue":"1"}'
            ),
            "ctl00$MainContent$RadGrid3$ctl00$ctl03$ctl01$ChangePageSizeTextBox": "10",
            "ctl00_MainContent_RadGrid3_ctl00_ctl03_ctl01_"
            "ChangePageSizeTextBox_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"10",'
                '"valueAsString":"10","minValue":1,"maxValue":878,'
                '"lastSetTextBoxValue":"10"}'
            ),
            "ctl00_MainContent_RadGrid3_ClientState": "",
        }

        # Process each letter A-Z
        logger.info("Starting A-Z search loop...")
        for letter in "abcdefghijklmnopqrstuvwxyz":
            logger.info(f"Processing committees starting with '{letter}'...")

            # Update payload with current letter
            payload = base_payload.copy()
            payload["ctl00$MainContent$commtypeBox"] = letter

            # Make the search request
            final_response = session.post(ND_SEARCH_URL, headers=HEADERS, data=payload)
            final_response.raise_for_status()

            # Make CSV export request
            logger.info(f"Exporting CSV for committees starting with '{letter}'...")
            export_payload = payload.copy()
            export_payload["__EVENTTARGET"] = "ctl00$MainContent$mngBtnCSV"
            export_payload["__VIEWSTATE"] = nd_extract_hidden_input_value(
                final_response.text, "__VIEWSTATE"
            )
            export_payload["__VIEWSTATEGENERATOR"] = nd_extract_hidden_input_value(
                final_response.text, "__VIEWSTATEGENERATOR"
            )
            export_payload["__EVENTVALIDATION"] = nd_extract_hidden_input_value(
                final_response.text, "__EVENTVALIDATION"
            )

            export_response = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=export_payload
            )
            export_response.raise_for_status()

            # Save cleaned CSV for each letter
            csv_file = base_dir / f"{year}_{letter}_committees.csv"
            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(export_response.text)

            logger.info(
                f"Committees Csv File Downloaded For Year {year}\
                    With Letter {letter} On Path {csv_file}"
            )


def clean_committee_data(row: list[str]) -> list[str]:
    """Clean and format committee data row.

    This function:
    1. Removes the year column from the beginning
    2. Ensures proper column structure
    3. Handles committee names that may contain commas

    Args:
        row (list[str]): Raw row data from CSV

    Returns:
        list[str]: Cleaned and formatted row data with proper column structure
    """
    # Clean each field by stripping whitespace
    cleaned = [field.strip() for field in row]

    # Remove year column (first column) and keep the rest
    if len(cleaned) > 0:
        cleaned = cleaned[1:]

    # Ensure we have exactly 8 columns
    if len(cleaned) < 8:
        # Pad with empty strings if needed
        cleaned.extend([""] * (8 - len(cleaned)))
    elif len(cleaned) > 8:
        # Take only the first 8 columns
        cleaned = cleaned[:8]

    return cleaned


@dg.asset(deps=[nd_fetch_candidates_data])
def nd_inserting_candidates_data(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """Insert North Dakota candidate data into the landing table.

    This function depends on nd_fetch_candidates_data and inserts the downloaded
    candidate data into the nd_candidates_landing table.

    Args:
        pg (dg.ResourceParam[PostgresResource]): The PostgreSQL resource for
            database operations.

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing:
            - The table name
            - The number of rows inserted
    """
    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="nd_candidates_landing",
        table_columns_name=ND_CANDIDATE_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(ND_CANDIDATE_COLUMNS)),
        category="candidates",
    )


@dg.asset(deps=[nd_fetch_committees_data])
def nd_inserting_committees_data(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """Insert North Dakota committees data into the landing table.

    This function depends on nd_fetch_committees_data and inserts the downloaded
    committees data into the nd_committees_landing table.

    Args:
        pg (dg.ResourceParam[PostgresResource]): The PostgreSQL resource for
            database operations.

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing:
            - The table name
            - The number of rows inserted
    """
    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="nd_committees_landing",
        table_columns_name=ND_COMMITTEE_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(ND_COMMITTEE_COLUMNS)),
        category="committees",
    )


# ==================
# North Dakota Contributions and Expenditures Data Ingestion
# ==================


@dg.asset(deps=[nd_get_initialize_search_session])
def nd_fetch_contributions_data(
    nd_get_initialize_search_session: tuple[requests.Session, str],
) -> None:
    """Fetch North Dakota contributions data by submitting search form.

    This function depends on nd_get_initialize_search_session and:
    1. Selects specific year so that it can show the contribution search
    2. Selects contribution search
    2. For each letter A-Z:
       - Makes search request
       - Exports to CSV
       - Cleans and saves the data
    """
    logger = dg.get_dagster_logger("nd_fetch_contributions_data")
    session, initial_response = nd_get_initialize_search_session

    # Create output directory if it doesn't exist
    output_dir = Path(ND_DATA_PATH_PREFIX) / "contributions"
    output_dir.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now().year

    for year in range(2014, current_year + 1):
        logger.info(f"Making first POST request to select for the year {year}")
        year_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$rcbxOnlyYear"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "__EVENTTARGET": "ctl00$MainContent$rcbxOnlyYear",
            "__EVENTARGUMENT": '{"Command":"Select","Index":1}',
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                initial_response, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": (
                f"{{"
                f'"logEntries":[],'
                f'"value":"{year}",'
                f'"text":"{year}",'
                f'"enabled":true,'
                f'"checkedIndices":[],'
                f'"checkedItemsTextOverflows":false'
                f"}}"
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":false'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        year_response = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=year_payload
        )
        year_response.raise_for_status()

        # Select contribution search
        logger.info("Making second POST request to select contribution search...")
        contr_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$radio1"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
            "ctl00$MainContent$searchCriteria": "radio1",
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__EVENTTARGET": "ctl00$MainContent$radio1",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                year_response.text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                year_response.text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                year_response.text, "__EVENTVALIDATION"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        contr_search = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=contr_payload
        )
        contr_search.raise_for_status()

        logger.info("Starting a-z search loop...")

        for letter in "abcdefghijklmnopqrstuvwxyz":
            base_payload = {
                "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                    initial_response
                ),
                "ctl00_rwndSubContributors_ClientState": "",
                "ctl00_RadWindowManager1_ClientState": "",
                "ctl00$txtSearch": "",
                "ctl00_MainContent_RadToolTipManager1_ClientState": "",
                "ctl00$MainContent$rcbxOnlyYear": str(year),
                "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
                "ctl00$MainContent$searchCriteria": "radio1",
                "ctl00$MainContent$ContributionsMRBy": "rbContributionsMadeBy",
                "ctl00$MainContent$nameBox": letter,
                "ctl00$MainContent$txtContRB": "",
                "ctl00$MainContent$cityBox": "",
                "ctl00$MainContent$stateComboBox": "",
                "ctl00_MainContent_stateComboBox_ClientState": "",
                "ctl00$MainContent$zipBox": "",
                "ctl00_MainContent_searchButton_ClientState": (
                    "{"
                    '"text":"Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_clearButton_ClientState": (
                    "{"
                    '"text":"Clear",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                    "{"
                    '"text":"Open New Window Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":false,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "__EVENTTARGET": "ctl00$MainContent$searchButton",
                "__LASTFOCUS": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": nd_extract_hidden_input_value(
                    contr_search.text, "__VIEWSTATE"
                ),
                "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                    contr_search.text, "__VIEWSTATEGENERATOR"
                ),
                "__EVENTVALIDATION": nd_extract_hidden_input_value(
                    contr_search.text, "__EVENTVALIDATION"
                ),
            }

            base_response = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=base_payload
            )
            base_response.raise_for_status()

            export_payload = {
                "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                    initial_response
                ),
                "__EVENTTARGET": "ctl00$MainContent$mngBtnCSV",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": nd_extract_hidden_input_value(
                    base_response.text, "__VIEWSTATE"
                ),
                "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                    base_response.text, "__VIEWSTATEGENERATOR"
                ),
                "__EVENTVALIDATION": nd_extract_hidden_input_value(
                    base_response.text, "__EVENTVALIDATION"
                ),
                "ctl00_rwndSubContributors_ClientState": "",
                "ctl00_RadWindowManager1_ClientState": "",
                "ctl00$txtSearch": "",
                "ctl00$MainContent$ScriptManager1": "",
                "ctl00_MainContent_RadToolTipManager1_ClientState": "",
                "ctl00$MainContent$rcbxOnlyYear": str(year),
                "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
                "ctl00$MainContent$searchCriteria": "radio1",
                "ctl00$MainContent$ContributionsMRBy": "rbContributionsMadeBy",
                "ctl00$MainContent$nameBox": letter,
                "ctl00$MainContent$txtContRB": "",
                "ctl00$MainContent$cityBox:": "",
                "ctl00_MainContent_stateComboBox_ClientState": "",
                "ctl00$MainContent$zipBox": "",
                "ctl00_MainContent_mngBtnCSV_ClientState": (
                    "{"
                    '"text":"Export Excel",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_searchButton_ClientState": (
                    "{"
                    '"text":"Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_clearButton_ClientState": (
                    "{"
                    '"text":"Clear",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                    "{"
                    '"text":"Open New Window Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":false,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00$MainContent$RadGrid1$ctl00$ctl03$ctl01$GoToPageTextBox": "1",
                "ctl00_MainContent_RadGrid1_ctl00_ctl03_ctl01_"
                "GoToPageTextBox_ClientState": (
                    '{"enabled":true,'
                    '"emptyMessage":"",'
                    '"validationText":"1",'
                    '"valueAsString":"1",'
                    '"minValue":1,'
                    '"maxValue":401,'
                    '"lastSetTextBoxValue":"1"'
                    "}"
                ),
                "ctl00$MainContent$RadGrid1$ctl00$ctl03$ctl01$"
                "ChangePageSizeTextBox": "10",
                "ctl00_MainContent_RadGrid1_ctl00_ctl03_ctl01_"
                "ChangePageSizeTextBox_ClientState": (
                    '{"enabled":true,'
                    '"emptyMessage":"",'
                    '"validationText":"10",'
                    '"valueAsString":"10",'
                    '"minValue":1,'
                    '"maxValue":4003,'
                    '"lastSetTextBoxValue":"10"'
                    "}"
                ),
                "ctl00_MainContent_RadGrid1_ClientState": "",
            }

            export_reponse = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=export_payload
            )
            export_reponse.raise_for_status()

            csv_file = output_dir / f"{year}_{letter}_contributions.csv"

            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(export_reponse.text)

            logger.info(
                f"Contributions Csv File Downloaded For Year {year}\
                    With Letter {letter} On Path {csv_file}"
            )


@dg.asset(deps=[nd_fetch_contributions_data])
def nd_inserting_contributions_data(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """Insert North Dakota contributions data into the landing table.

    This function depends on nd_fetch_contributions_data and inserts the downloaded
    contributions data into the nd_contributions_landing table.

    Args:
        pg (dg.ResourceParam[PostgresResource]): The PostgreSQL resource for
            database operations.

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing:
            - The table name
            - The number of rows inserted
    """
    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="nd_contributions_landing",
        table_columns_name=ND_CONTRIBUTION_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(ND_CONTRIBUTION_COLUMNS)),
        category="contributions",
    )


@dg.asset(deps=[nd_get_initialize_search_session])
def nd_fetch_expenditures_data(
    nd_get_initialize_search_session: tuple[requests.Session, str],
) -> None:
    """Fetch North Dakota expenditures data by submitting search form.

    This function depends on nd_get_initialize_search_session and:
    1. Selects specific year so that it can show the expenditure search
    2. Selects expenditure search
    2. For each letter A-Z:
       - Makes search request
       - Exports to CSV
       - Cleans and saves the data
    """
    logger = dg.get_dagster_logger("nd_fetch_expenditures_data")
    session, initial_response = nd_get_initialize_search_session

    # Create output directory if it doesn't exist
    output_dir = Path(ND_DATA_PATH_PREFIX) / "expenditures"
    output_dir.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now().year

    for year in range(2014, current_year + 1):
        logger.info(f"Making first POST request to select for the year {year}")
        year_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$rcbxOnlyYear"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "__EVENTTARGET": "ctl00$MainContent$rcbxOnlyYear",
            "__EVENTARGUMENT": '{"Command":"Select","Index":1}',
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                initial_response, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                initial_response, "__EVENTVALIDATION"
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": (
                f"{{"
                f'"logEntries":[],'
                f'"value":"{year}",'
                f'"text":"{year}",'
                f'"enabled":true,'
                f'"checkedIndices":[],'
                f'"checkedItemsTextOverflows":false'
                f"}}"
            ),
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":false'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        year_response = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=year_payload
        )
        year_response.raise_for_status()

        # Select expenditure search
        logger.info("Making second POST request to select expenditure search...")
        expend_payload = {
            "ctl00$MainContent$ScriptManager1": (
                "ctl00$ctl00$RadAjaxPanel1Panel|ctl00$MainContent$radio2"
            ),
            "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                initial_response
            ),
            "ctl00_rwndSubContributors_ClientState": "",
            "ctl00_RadWindowManager1_ClientState": "",
            "ctl00$txtSearch": "",
            "ctl00_MainContent_RadToolTipManager1_ClientState": "",
            "ctl00$MainContent$rcbxOnlyYear": str(year),
            "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
            "ctl00$MainContent$searchCriteria": "radio2",
            "ctl00_MainContent_searchButton_ClientState": (
                "{"
                '"text":"Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_clearButton_ClientState": (
                "{"
                '"text":"Clear",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":true,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                "{"
                '"text":"Open New Window Search",'
                '"value":"",'
                '"checked":false,'
                '"target":"",'
                '"navigateUrl":"",'
                '"commandName":"",'
                '"commandArgument":"",'
                '"autoPostBack":false,'
                '"selectedToggleStateIndex":0,'
                '"validationGroup":null,'
                '"readOnly":false,'
                '"primary":false,'
                '"enabled":true'
                "}"
            ),
            "__EVENTTARGET": "ctl00$MainContent$radio2",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": nd_extract_hidden_input_value(
                year_response.text, "__VIEWSTATE"
            ),
            "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                year_response.text, "__VIEWSTATEGENERATOR"
            ),
            "__EVENTVALIDATION": nd_extract_hidden_input_value(
                year_response.text, "__EVENTVALIDATION"
            ),
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "ctl00_RadAjaxPanel1",
        }

        expend_search = session.post(
            ND_SEARCH_URL, headers=POST_HEADERS, data=expend_payload
        )
        expend_search.raise_for_status()

        logger.info("Starting a-z search loop...")

        for letter in "abcdefghijklmnopqrstuvwxyz":
            base_payload = {
                "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                    initial_response
                ),
                "ctl00_rwndSubContributors_ClientState": "",
                "ctl00_RadWindowManager1_ClientState": "",
                "ctl00$txtSearch": "",
                "ctl00_MainContent_RadToolTipManager1_ClientState": "",
                "ctl00$MainContent$rcbxOnlyYear": str(year),
                "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
                "ctl00$MainContent$searchCriteria": "radio2",
                "ctl00$MainContent$ExpendituresMRBy": "rbExpenditureMadeBy",
                "ctl00$MainContent$recipBox": letter,
                "ctl00$MainContent$txtExpRB": "",
                "ctl00$MainContent$cityBoxRec": "",
                "ctl00$MainContent$stateComboBoxRec": "",
                "ctl00_MainContent_stateComboBoxRec_ClientState:": "",
                "ctl00$MainContent$zipBoxRec": "",
                "ctl00_MainContent_searchButton_ClientState": (
                    "{"
                    '"text":"Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_clearButton_ClientState": (
                    "{"
                    '"text":"Clear",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                    "{"
                    '"text":"Open New Window Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":false,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "__EVENTTARGET": "ctl00$MainContent$searchButton",
                "__LASTFOCUS": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": nd_extract_hidden_input_value(
                    expend_search.text, "__VIEWSTATE"
                ),
                "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                    expend_search.text, "__VIEWSTATEGENERATOR"
                ),
                "__EVENTVALIDATION": nd_extract_hidden_input_value(
                    expend_search.text, "__EVENTVALIDATION"
                ),
            }

            base_response = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=base_payload
            )
            base_response.raise_for_status()

            export_payload = {
                "MainContent_ScriptManager1_TSM": nd_extract_radscriptmanager_script(
                    initial_response
                ),
                "__EVENTTARGET": "ctl00$MainContent$mngBtnCSV",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": nd_extract_hidden_input_value(
                    base_response.text, "__VIEWSTATE"
                ),
                "__VIEWSTATEGENERATOR": nd_extract_hidden_input_value(
                    base_response.text, "__VIEWSTATEGENERATOR"
                ),
                "__EVENTVALIDATION": nd_extract_hidden_input_value(
                    base_response.text, "__EVENTVALIDATION"
                ),
                "ctl00_rwndSubContributors_ClientState": "",
                "ctl00_RadWindowManager1_ClientState": "",
                "ctl00$txtSearch": "",
                "ctl00$MainContent$ScriptManager1": "",
                "ctl00_MainContent_RadToolTipManager1_ClientState": "",
                "ctl00$MainContent$rcbxOnlyYear": str(year),
                "ctl00_MainContent_rcbxOnlyYear_ClientState": "",
                "ctl00$MainContent$searchCriteria": "radio2",
                "ctl00$MainContent$ExpendituresMRBy": "rbExpenditureMadeBy",
                "ctl00$MainContent$recipBox": letter,
                "ctl00$MainContent$txtExpRB": "",
                "ctl00$MainContent$cityBoxRec": "",
                "ctl00$MainContent$stateComboBoxRec": "",
                "ctl00_MainContent_stateComboBoxRec_ClientState": "",
                "ctl00$MainContent$zipBoxRec": "",
                "ctl00_MainContent_mngBtnCSV_ClientState": (
                    "{"
                    '"text":"Export Excel",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_searchButton_ClientState": (
                    "{"
                    '"text":"Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_clearButton_ClientState": (
                    "{"
                    '"text":"Clear",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":true,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00_MainContent_rbtnNewSearchWindow_ClientState": (
                    "{"
                    '"text":"Open New Window Search",'
                    '"value":"",'
                    '"checked":false,'
                    '"target":"",'
                    '"navigateUrl":"",'
                    '"commandName":"",'
                    '"commandArgument":"",'
                    '"autoPostBack":false,'
                    '"selectedToggleStateIndex":0,'
                    '"validationGroup":null,'
                    '"readOnly":false,'
                    '"primary":false,'
                    '"enabled":true'
                    "}"
                ),
                "ctl00$MainContent$RadGrid2$ctl00$ctl03$ctl01$GoToPageTextBox": "1",
                "ctl00_MainContent_RadGrid2_ctl00_ctl03_ctl01_"
                "GoToPageTextBox_ClientState": (
                    '{"enabled":true,'
                    '"emptyMessage":"",'
                    '"validationText":"1",'
                    '"valueAsString":"1",'
                    '"minValue":1,'
                    '"maxValue":170,'
                    '"lastSetTextBoxValue":"1"}'
                ),
                "ctl00$MainContent$RadGrid2$ctl00$ctl03$ctl01$"
                "ChangePageSizeTextBox": "10",
                "ctl00_MainContent_RadGrid2_ctl00_ctl03_ctl01_"
                "ChangePageSizeTextBox_ClientState": (
                    '{"enabled":true,'
                    '"emptyMessage":"",'
                    '"validationText":"10",'
                    '"valueAsString":"10",'
                    '"minValue":1,'
                    '"maxValue":1698,'
                    '"lastSetTextBoxValue":"10"'
                    "}"
                ),
                "ctl00_MainContent_RadGrid2_ClientState": "",
            }

            export_reponse = session.post(
                ND_SEARCH_URL, headers=HEADERS, data=export_payload
            )
            export_reponse.raise_for_status()

            csv_file = output_dir / f"{year}_{letter}_expenditures.csv"

            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(export_reponse.text)

            logger.info(
                f"Csv File Downloaded For Year {year}\
                    With Letter {letter} On Path {csv_file}"
            )


@dg.asset(deps=[nd_fetch_expenditures_data])
def nd_inserting_expenditures_data(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """Insert North Dakota expenditures data into the landing table.

    This function depends on nd_fetch_expenditures_data and inserts the downloaded
    expenditures data into the nd_expenditures_landing table.

    Args:
        pg (dg.ResourceParam[PostgresResource]): The PostgreSQL resource for
            database operations.

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing:
            - The table name
            - The number of rows inserted
    """
    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="nd_expenditures_landing",
        table_columns_name=ND_EXPENDITURE_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(ND_EXPENDITURE_COLUMNS)),
        category="expenditures",
    )

import csv
import glob
import json
import os
import re
from collections.abc import Callable
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

WI_DATA_PATH_PREFIX = "./states/wisconsin"

WI_BASE_URL = "https://cfis.wi.gov"

WI_FILED_REPORTS_URL = f"{WI_BASE_URL}/Public/FiledReports.aspx"
WI_FILED_REPORTS_PUBLIC_NOTE_URL = (
    f"{WI_BASE_URL}/Public/PublicNote.aspx?Page=FiledReports"
)

WI_RECEIPT_TRANSACTIONS_URL = f"{WI_BASE_URL}/Public/ReceiptList.aspx"
WI_RECEIPT_TRANSACTIONS_PUBLIC_NOTE_URL = (
    f"{WI_BASE_URL}/Public/PublicNote.aspx?Page=ReceiptList"
)
WI_EXPENSE_TRANSACTIONS_URL = f"{WI_BASE_URL}/Public/ExpenseList.aspx"
WI_EXPENSE_TRANSACTIONS_PUBLIC_NOTE_URL = (
    f"{WI_BASE_URL}/Public/PublicNote.aspx?Page=ExpenseList"
)

WI_REGISTERED_COMMITTEES_REGISTRATION_URL = (
    f"{WI_BASE_URL}/Public/Registration.aspx?page=RegistrantList"
)

WI_REGISTERED_COMMITTEES_URL = f"{WI_BASE_URL}/Public/RegistrantList.aspx"
WI_REGISTERED_COMMITTEES_PUBLIC_NOTE_URL = (
    f"{WI_BASE_URL}/Public/PublicNote.aspx?Page=RegistrantList"
)

WI_HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    ),
    "accept-language": "en-US,en;q=0.9",
    "accept-encoding": "gzip, deflate, br, zstd",
    "connection": "keep-alive",
    "referer": WI_BASE_URL + "/Public/Registration.aspx?page=FiledReports",
    "upgrade-insecure-requests": "1",
}


# -------------------------------
# Utilities
# -------------------------------


def wi_extract_hidden_input(html_content: str, key: str) -> str:
    """
    Extracts the value of a hidden input field by its 'name' attribute from the
    HTML content.

    Parameters:
    - html_content (str): The HTML content to search within.
    - key (str): The 'name' attribute of the hidden input field to find.

    Returns:
    - str: The value of the hidden input field, or an empty string if not found.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    element = soup.find("input", {"type": "hidden", "name": key})

    if isinstance(element, Tag) and element.has_attr("value"):
        return str(element["value"])  # Explicit cast for type safety

    return ""


def wi_extract_years(
    html: str, dropdown_id: str = "cmbFilingYear_DropDown"
) -> list[str]:
    """
    Extracts a list of years from a dropdown menu in the given HTML content.

    This function looks for a <div> element with the specified dropdown ID,
    and extracts the text of all <li> elements within
    it that contain numeric values (e.g., "2023").

    Args:
        html (str): The raw HTML content of the page.
        dropdown_id (str): The ID of the dropdown <div> to search within.
            Defaults to "cmbFilingYear_DropDown".

    Returns:
        list[str]: A list of year strings extracted from the dropdown.
    """
    soup = BeautifulSoup(html, "html.parser")
    dropdown = soup.find("div", id=dropdown_id)

    if not isinstance(dropdown, Tag):
        return []

    return [
        li.text.strip() for li in dropdown.select("li") if li.text.strip().isdigit()
    ]


def wi_extract_filing_calendar(html_content: str) -> dict[str, str]:
    """
    Extracts hidden input values related to filing calendar names from the given
    HTML content.

    This function parses the HTML to find all hidden <input> elements
    and filters for those
    whose 'name' attribute starts with 'cmbFilingCalenderName'.
    It then returns a dictionary
    mapping the 'name' attribute to its corresponding 'value'.

    Args:
        html_content (str): Raw HTML content from the Wisconsin campaign
        finance disclosure page.

    Returns:
        Dict[str, str]: A dictionary where keys are input names and
        values are their associated values.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    inputs = soup.find_all("input", attrs={"type": "hidden"})

    result = {}
    for input_tag in inputs:
        if isinstance(input_tag, Tag):
            name = input_tag.get("name")
            if (
                isinstance(name, str)
                and name.startswith("cmbFilingCalenderName")
                and input_tag.has_attr("value")
            ):
                result[name] = input_tag["value"]
    return result


def wi_extract_registrant_types(html_content):
    """
    Extracts the list of registrant types from the provided Wisconsin HTML content.

    This function parses the HTML to find the list items (<li>) within the dropdown
    element for registrant types (identified by the CSS selector
            '#cmbType_DropDownul.rcbList').
    It returns a list of non-empty text values.

    Args:
        html_content (str): Raw HTML content from the Wisconsin
        campaign finance disclosure page.

    Returns:
        List[str]: A list of registrant type strings.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    return [
        li.get_text(strip=True)
        for li in soup.select("#cmbType_DropDown ul.rcbList li")
        if li.get_text(strip=True)
    ]


def wi_extract_radscriptmanager_script(html_content: str) -> str:
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
    return ""  # Return an empty string if the script tag is not found


def wi_create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(WI_HEADERS)
    return session


def wi_insert_raw_file_to_landing_table(
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

        data_validation_callback (Callable): A function that takes a CSV row and returns
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

    base_path = Path(WI_DATA_PATH_PREFIX)
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
                    data_type_lines_generator, delimiter=","
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


# -------------------------------
# Dagster Assets For Filed Reports
# -------------------------------


@dg.asset
def wi_filed_reports_get_session() -> dict:
    """
    Initializes a session and retrieves the initial session cookies and headers
    from the Wisconsin Public Registration Filed Reports page.

    Returns:
        dict: Dictionary containing `cookies` and `headers` for the session.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    res = session.get(f"{WI_BASE_URL}/Public/Registration.aspx?page=FiledReports")
    logger.info(f"Initial GET status: {res.status_code}")

    return {"cookies": session.cookies.get_dict(), "headers": dict(session.headers)}


@dg.asset(deps=["wi_filed_reports_get_session"])
def wi_filed_reports_get_intermediate_note(wi_filed_reports_get_session: dict) -> dict:
    """
    Performs a follow-up GET request to the intermediate 'note' page required
    before accessing filed reports. Extracts updated cookies, headers, and
    necessary hidden form fields (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`).

    Args:
        wi_filed_reports_get_session (dict): Session metadata from the initial GET.

    Returns:
        dict: Updated session metadata and extracted hidden form field values.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_filed_reports_get_session["cookies"])

    res = session.get(WI_FILED_REPORTS_PUBLIC_NOTE_URL)
    logger.info(f"Note page GET status: {res.status_code}")

    html = res.text
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
        "viewstate": wi_extract_hidden_input(html, "__VIEWSTATE"),
        "viewstategenerator": wi_extract_hidden_input(html, "__VIEWSTATEGENERATOR"),
    }


@dg.asset(deps=["wi_filed_reports_get_intermediate_note"])
def wi_filed_reports_get_post_continue(wi_filed_reports_get_intermediate_note: dict):
    """
    Submits a POST request to continue past the intermediate note page, simulating
    the user clicking the "Continue" button. This step is necessary to unlock
    access to the actual report search page.

    Args:
        wi_filed_reports_get_intermediate_note (dict): Metadata including cookies,
        headers, and hidden form fields from the note page.

    Returns:
        dict: Updated session metadata after the POST request.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_filed_reports_get_intermediate_note["cookies"])
    session.headers.update(wi_filed_reports_get_intermediate_note["headers"])

    res = session.post(
        WI_FILED_REPORTS_PUBLIC_NOTE_URL,
        data={
            "__LASTFOCUS": "",
            "__VIEWSTATE": wi_filed_reports_get_intermediate_note["viewstate"],
            "__VIEWSTATEGENERATOR": (
                wi_filed_reports_get_intermediate_note["viewstategenerator"]
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "btnContinue.x": "55",
            "btnContinue.y": "19",
        },
    )

    logger.info(f"POST continue status: {res.status_code}")
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
    }


@dg.asset(deps=["wi_filed_reports_get_post_continue"])
def wi_filed_reports_get_final_html(wi_filed_reports_get_post_continue: dict):
    """
    Accesses the final Filed Reports page, extracts available filing years,
    simulates selecting each year, performs a search, and then downloads the
    corresponding CSV file for each year.

    Args:
        wi_filed_reports_get_post_continue (dict): Session cookies and headers
        after continuing from the note page

    Returns:
        str: Placeholder string (optionally could return a list of file paths
        or summary if needed); main purpose is side-effect of downloading CSVs.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_filed_reports_get_post_continue["cookies"])
    session.headers.update(wi_filed_reports_get_post_continue["headers"])

    res = session.get(WI_FILED_REPORTS_URL)
    logger.info(f"Final GET status: {res.status_code}")

    html_content = res.text

    years = wi_extract_years(html_content, "cmbFilingYear_DropDown")
    script_src = wi_extract_radscriptmanager_script(html_content)
    viewstate = wi_extract_hidden_input(html_content, "__VIEWSTATE")
    viewstategenerator = wi_extract_hidden_input(html_content, "__VIEWSTATEGENERATOR")

    logger.info(f"script_src: {script_src}")
    logger.info(f"viewstategenerator: {viewstategenerator}")

    for year in years:
        selected_year_index = years.index(year) + 1

        payload = {
            "RadScriptManager1": "cfis_ajxPanelPanel|cmbFilingYear",
            "__LASTFOCUS": "",
            "RadScriptManager1_TSM": script_src,
            "__EVENTTARGET": "cmbFilingYear",
            "__EVENTARGUMENT": f'{{"Command":"Select","Index":{selected_year_index}}}',
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "cmbFilingYear": str(year),
            "cmbFilingYear_ClientState": (
                f'{{"logEntries":[],"value":"{year}","text":"{year}","enabled":true,"checkedIndices":[],"checkedItemsTextOverflows":false}}'
            ),
            "cmbFilingPeriodName": "All Filing Periods",
            "cmbFilingPeriodName_ClientState": "",
            "cmbReportName": "",
            "cmbReportName_ClientState": "",
            "cmbRegistrantType": "",
            "cmbRegistrantType_ClientState": "",
            "txtGABID": "",
            "txtGABID_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}'
            ),
            "cmbRegistrantName": "",
            "cmbRegistrantName_ClientState": "",
            "cmbOffice": "",
            "cmbOffice_ClientState": "",
            "cmbDistrict": "",
            "cmbDistrict_ClientState": "",
            "cmbBranch": "",
            "cmbBranch_ClientState": "",
            "dtpFromDate": "",
            "dtpFromDate$dateInput": "",
            "dtpFromDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpFromDate_calendar_SD": "[]",
            "dtpFromDate_calendar_AD": "[[1980,1,1],[2099,12,30],[2025,4,8]]",
            "dtpFromDate_ClientState": "",
            "dtpToDate": "",
            "dtpToDate$dateInput": "",
            "dtpToDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpToDate_calendar_SD": "[]",
            "dtpToDate_calendar_AD": "[[1980,1,1],[2099,12,30],[2025,4,8]]",
            "dtpToDate_ClientState": "",
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "cfis_ajxPanel",
        }

        # Important for ASP.NET: Add this special header
        session.headers.update(
            {
                "X-MicrosoftAjax": "Delta=true",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": WI_FILED_REPORTS_URL,
                "Origin": "https://cfis.wi.gov",
            }
        )

        # Select year
        session.post(WI_FILED_REPORTS_URL, data=payload)

        # Search
        search_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|btnSearch",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "btnSearch.x": "39",
            "btnSearch.y": "11",
        }
        session.post(WI_FILED_REPORTS_URL, data=search_payload, stream=True)

        # Download CSV
        csv_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|cmdText",
            "__EVENTTARGET": "cmdText",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
        res_csv = session.post(WI_FILED_REPORTS_URL, data=csv_payload)
        logger.info(f"CSV POST status for {year}: {res_csv.status_code}")

        if "attachment" in res_csv.headers.get("content-disposition", ""):
            data_path = Path(WI_DATA_PATH_PREFIX) / "filter_reports"
            data_path.mkdir(parents=True, exist_ok=True)
            file_path = data_path / f"{year}_filter_reports.csv"
            with open(file_path, "wb") as f:
                f.write(res_csv.content)
            logger.info(f"✅ CSV saved to {file_path}")
        else:
            logger.warning(f"❌ Failed to download CSV for year {year}")


@dg.asset(deps=["wi_filed_reports_get_final_html"])
def wi_filed_reports_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Wisconsin filed reports CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_filter_reports.csv` files inside the
        `filter_reports/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return wi_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="wi_filed_reports_landing",
        table_columns_name=[
            "filing_period_name",
            "report_type_code",
            "report_name",
            "registrant_type",
            "date_filed",
            "amended_date",
            "committee_id",
            "registrant_name",
            "election_year",
            "branch",
            "filing_year",
            "extension",
            "comment_flag",
            "attachment",
            "view_report_type",
            "created_date",
            "empty_column",
        ],
        data_validation_callback=lambda row: len(row) == 17,
        category="filter_reports",
    )


# -------------------------------
# Dagster Assets For Receipt Transaction
# -------------------------------


@dg.asset
def wi_receipt_transactions_get_session() -> dict:
    """
    Initializes a session and retrieves the initial session cookies and headers
    from the Wisconsin Public Registration Filed Reports page.

    Returns:
        dict: Dictionary containing `cookies` and `headers` for the session.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    res = session.get(f"{WI_BASE_URL}/Public/Registration.aspx?page=ReceiptList")
    logger.info(f"Initial GET status: {res.status_code}")

    return {"cookies": session.cookies.get_dict(), "headers": dict(session.headers)}


@dg.asset(deps=["wi_receipt_transactions_get_session"])
def wi_receipt_transactions_get_intermediate_note(
    wi_receipt_transactions_get_session: dict,
) -> dict:
    """
    Performs a follow-up GET request to the intermediate 'note' page required
    before accessing filed reports. Extracts updated cookies, headers, and
    necessary hidden form fields (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`).

    Args:
        wi_receipt_transactions_get_session (dict): Session metadata from the
        initial GET.

    Returns:
        dict: Updated session metadata and extracted hidden form field values.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_receipt_transactions_get_session["cookies"])

    res = session.get(WI_RECEIPT_TRANSACTIONS_PUBLIC_NOTE_URL)
    logger.info(f"Note page GET status: {res.status_code}")

    html = res.text
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
        "viewstate": wi_extract_hidden_input(html, "__VIEWSTATE"),
        "viewstategenerator": wi_extract_hidden_input(html, "__VIEWSTATEGENERATOR"),
    }


@dg.asset(deps=["wi_receipt_transactions_get_intermediate_note"])
def wi_receipt_transactions_get_post_continue(
    wi_receipt_transactions_get_intermediate_note: dict,
):
    """
    Submits a POST request to continue past the intermediate note page, simulating
    the user clicking the "Continue" button. This step is necessary to unlock
    access to the actual report search page.

    Args:
        wi_receipt_transactions_get_intermediate_note (dict): Metadata including
        cookies, headers, and hidden form fields from the note page.

    Returns:
        dict: Updated session metadata after the POST request.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_receipt_transactions_get_intermediate_note["cookies"])
    session.headers.update(wi_receipt_transactions_get_intermediate_note["headers"])

    res = session.post(
        WI_RECEIPT_TRANSACTIONS_PUBLIC_NOTE_URL,
        data={
            "__LASTFOCUS": "",
            "__VIEWSTATE": wi_receipt_transactions_get_intermediate_note["viewstate"],
            "__VIEWSTATEGENERATOR": (
                wi_receipt_transactions_get_intermediate_note["viewstategenerator"]
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "btnContinue.x": "55",
            "btnContinue.y": "19",
        },
    )

    logger.info(f"POST continue status: {res.status_code}")
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
    }


@dg.asset(deps=["wi_receipt_transactions_get_post_continue"])
def wi_receipt_transactions_get_final_html(
    wi_receipt_transactions_get_post_continue: dict,
):
    """
    Accesses the final Filed Reports page, extracts available filing years,
    simulates selecting each year, performs a search, and then downloads the
    corresponding CSV file for each year.

    Args:
        wi_receipt_transactions_get_post_continue (dict): Session cookies and headers
        after continuing from the note page

    Returns:
        str: Placeholder string (optionally could return a list of file paths
        or summary if needed); main purpose is side-effect of downloading CSVs.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_receipt_transactions_get_post_continue["cookies"])
    session.headers.update(wi_receipt_transactions_get_post_continue["headers"])

    res = session.get(WI_RECEIPT_TRANSACTIONS_URL)
    logger.info(f"Final GET status: {res.status_code}")

    html_content = res.text

    years = wi_extract_years(html_content, "cmbFilingYear_DropDown")
    script_src = wi_extract_radscriptmanager_script(html_content)
    viewstate = wi_extract_hidden_input(html_content, "__VIEWSTATE")
    viewstategenerator = wi_extract_hidden_input(html_content, "__VIEWSTATEGENERATOR")

    logger.info(f"script_src: {script_src}")
    logger.info(f"viewstategenerator: {viewstategenerator}")

    for year in years:
        selected_year_index = years.index(year) + 1

        payload = {
            "RadScriptManager1": "cfis_ajxPanelPanel|cmbFilingYear",
            "RadScriptManager1_TSM": script_src,
            "cmbContributorType": "",
            "cmbContributorType_ClientState": "",
            "cmbContributionType": "",
            "cmbContributionType_ClientState": "",
            "txtContributorName": "",
            "txtContributorName_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":""'
                ',"lastSetTextBoxValue":""}'
            ),
            "txtFirstName": "",
            "txtFirstName_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":""'
                ',"lastSetTextBoxValue":""}'
            ),
            "cmbFilingYear": str(year),
            "cmbFilingYear_ClientState": (
                f'{{"logEntries":[],"value":"{year}","text":"{year}","enabled":true,'
                f'"checkedIndices":[],"checkedItemsTextOverflows":false}}'
            ),
            "cmbFilingCalenderName": "All Filing Periods",
            "cmbFilingCalenderName$i0$hfFilingID": "0",
            "cmbFilingCalenderName$i1$hfFilingID": "209",
            "cmbFilingCalenderName$i2$hfFilingID": "208",
            "cmbFilingCalenderName$i3$hfFilingID": "207",
            "cmbFilingCalenderName_ClientState": "",
            "txtOccupation": "",
            "txtOccupation_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":""'
                ',"lastSetTextBoxValue":""}'
            ),
            "txtGabid": "",
            "txtGabid_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":""'
                ',"lastSetTextBoxValue":""}'
            ),
            "cmbOffice": "",
            "cmbOffice_ClientState": "",
            "cmbDistrictCounty": "",
            "cmbDistrictCounty_ClientState": "",
            "cmbBranch": "",
            "cmbBranch_ClientState": "",
            "dtpDateStart": "",
            "dtpDateStart$dateInput": "",
            "dtpDateStart_dateInput_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00","lastSetTextBoxValue":""}'
            ),
            "dtpDateStart_calendar_SD": "[]",
            "dtpDateStart_calendar_AD": "[[1980,1,1],[2099,12,30],[2025,4,8]]",
            "dtpDateStart_ClientState": "",
            "NtxAmountStart": "",
            "NtxAmountStart_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","'
                'minValue":0,"maxValue":70368744177664,"lastSetTextBoxValue":""}'
            ),
            "NtxAmountEnd": "",
            "NtxAmountEnd_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","'
                'minValue":0,"maxValue":70368744177664,"lastSetTextBoxValue":""}'
            ),
            "cmbReceivingCommittee": "",
            "cmbReceivingCommittee_ClientState": "",
            "dtpDateEnd": "",
            "dtpDateEnd$dateInput": "",
            "dtpDateEnd_dateInput_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00","lastSetTextBoxValue":""}'
            ),
            "dtpDateEnd_calendar_SD": "[]",
            "dtpDateEnd_calendar_AD": "[[1980,1,1],[2099,12,30],[2025,4,8]]",
            "dtpDateEnd_ClientState": "",
            "grdReceipts_ClientState": "",
            "UserListDialog_ClientState": "",
            "RadWindowManager1_ClientState": "",
            "__LASTFOCUS": "",
            "__EVENTTARGET": "cmbFilingYear",
            "__EVENTARGUMENT": f'{{"Command":"Select","Index":{selected_year_index}}}',
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "cfis_ajxPanel",
        }

        # Important for ASP.NET: Add this special header
        session.headers.update(
            {
                "X-MicrosoftAjax": "Delta=true",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": WI_RECEIPT_TRANSACTIONS_URL,
                "Origin": "https://cfis.wi.gov",
            }
        )

        # Select year
        session.post(WI_RECEIPT_TRANSACTIONS_URL, data=payload)

        # Search
        search_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|btnSearch",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "btnSearch.x": "39",
            "btnSearch.y": "11",
        }
        session.post(WI_RECEIPT_TRANSACTIONS_URL, data=search_payload)

        # Download CSV
        csv_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|cmdText",
            "__EVENTTARGET": "cmdText",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
        res_csv = session.post(
            WI_RECEIPT_TRANSACTIONS_URL, data=csv_payload, stream=True
        )
        logger.info(f"CSV POST status for {year}: {res_csv.status_code}")

        if "attachment" in res_csv.headers.get("content-disposition", ""):
            data_path = Path(WI_DATA_PATH_PREFIX) / "receipt_transactions"
            data_path.mkdir(parents=True, exist_ok=True)
            file_path = data_path / f"{year}_receipt_transactions.csv"
            with open(file_path, "wb") as f:
                f.write(res_csv.content)
            logger.info(f"✅ CSV saved to {file_path}")
        else:
            logger.warning(f"❌ Failed to download CSV for year {year}")


@dg.asset(deps=["wi_receipt_transactions_get_final_html"])
def wi_receipt_transactions_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Wisconsin receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_receipt_transactions.csv` files inside the
        `receipt_transactions/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return wi_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="wi_receipt_transactions_landing",
        table_columns_name=[
            "transaction_date",
            "filing_period_name",
            "contributor_name",
            "contribution_amount",
            "address_line1",
            "address_line2",
            "city",
            "state_code",
            "zip",
            "occupation",
            "employer_name",
            "employer_address",
            "contributor_type",
            "receiving_committee_name",
            "ethcfid",
            "conduit",
            "branch",
            "comment",
            "report_72hr",
            "segregated_fund_flag",
            "empty_column",
        ],
        data_validation_callback=lambda row: len(row) == 21,
        category="receipt_transactions",
    )


# -------------------------------
# Dagster Assets For Expense Transaction
# -------------------------------


@dg.asset
def wi_expense_transactions_get_session() -> dict:
    """
    Initializes a session and retrieves the initial session cookies and headers
    from the Wisconsin Public Registration Filed Reports page.

    Returns:
        dict: Dictionary containing `cookies` and `headers` for the session.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()

    res = session.get(f"{WI_BASE_URL}/Public/Registration.aspx?page=ExpenseList")
    logger.info(f"Initial GET status: {res.status_code}")

    return {"cookies": session.cookies.get_dict(), "headers": dict(session.headers)}


@dg.asset(deps=["wi_expense_transactions_get_session"])
def wi_expense_transactions_get_intermediate_note(
    wi_expense_transactions_get_session: dict,
) -> dict:
    """
    Performs a follow-up GET request to the intermediate 'note' page required
    before accessing filed reports. Extracts updated cookies, headers, and
    necessary hidden form fields (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`).

    Args:
        wi_expense_transactions_get_session (dict): Session metadata from the
        initial GET.

    Returns:
        dict: Updated session metadata and extracted hidden form field values.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_expense_transactions_get_session["cookies"])

    res = session.get(WI_EXPENSE_TRANSACTIONS_PUBLIC_NOTE_URL)
    logger.info(f"Note page GET status: {res.status_code}")

    html = res.text
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
        "viewstate": wi_extract_hidden_input(html, "__VIEWSTATE"),
        "viewstategenerator": wi_extract_hidden_input(html, "__VIEWSTATEGENERATOR"),
    }


@dg.asset(deps=["wi_expense_transactions_get_intermediate_note"])
def wi_expense_transactions_get_post_continue(
    wi_expense_transactions_get_intermediate_note: dict,
) -> dict:
    """
    Submits a POST request to continue past the intermediate note page,
    simulating
    the user clicking the "Continue" button. This step is necessary to unlock
    access to the actual report search page.

    Args:
        wi_expense_transactions_get_intermediate_note (dict): Metadata including
        cookies, headers, and hidden form fields from the note page.

    Returns:
        dict: Updated session metadata after the POST request.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_expense_transactions_get_intermediate_note["cookies"])
    session.headers.update(wi_expense_transactions_get_intermediate_note["headers"])

    res = session.post(
        WI_EXPENSE_TRANSACTIONS_PUBLIC_NOTE_URL,
        data={
            "__LASTFOCUS": "",
            "__VIEWSTATE": wi_expense_transactions_get_intermediate_note["viewstate"],
            "__VIEWSTATEGENERATOR": (
                wi_expense_transactions_get_intermediate_note["viewstategenerator"]
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "btnContinue.x": "55",
            "btnContinue.y": "19",
        },
    )

    logger.info(f"POST continue status: {res.status_code}")
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
    }


@dg.asset(deps=["wi_expense_transactions_get_post_continue"])
def wi_expense_transactions_get_final_html(
    wi_expense_transactions_get_post_continue: dict,
):
    """
    Accesses the final Filed Reports page, extracts available filing years,
    simulates selecting each year, performs a search, and then downloads the
    corresponding CSV file for each year.

    Args:
        wi_expense_transactions_get_post_continue (dict): Session cookies and headers
        after continuing from the note page

    Returns:
        str: Placeholder string (optionally could return a list of file paths
        or summary if needed); main purpose is side-effect of downloading CSVs.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_expense_transactions_get_post_continue["cookies"])
    session.headers.update(wi_expense_transactions_get_post_continue["headers"])

    res = session.get(WI_EXPENSE_TRANSACTIONS_URL)
    logger.info(f"Final GET status: {res.status_code}")

    html_content = res.text

    years = wi_extract_years(html_content, "cmbFilingYear_DropDown")
    script_src = wi_extract_radscriptmanager_script(html_content)
    viewstate = wi_extract_hidden_input(html_content, "__VIEWSTATE")
    viewstategenerator = wi_extract_hidden_input(html_content, "__VIEWSTATEGENERATOR")

    filling_period_payload = wi_extract_filing_calendar(html_content)

    logger.info(f"script_src: {script_src}")
    logger.info(f"viewstategenerator: {viewstategenerator}")

    for year in years:
        selected_year_index = years.index(year) + 1

        payload = {
            "RadScriptManager1": "cfis_ajxPanelPanel|cmbFilingYear",
            "__LASTFOCUS": "",
            "RadScriptManager1_TSM": script_src,
            "__EVENTTARGET": "cmbFilingYear",
            "__EVENTARGUMENT": f'{{"Command":"Select","Index":{selected_year_index}}}',
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "cmbPayeeCommittee": "",
            "cmbPayeeCommittee_ClientState": "",
            "cmbExpenseType": "",
            "cmbExpenseType_ClientState": "",
            "txtPayeeName": "",
            **filling_period_payload,
            "txtPayeeName_ClientState": (
                '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}'
            ),
            "cmbOffice": "",
            "cmbOffice_ClientState": "",
            "cmbDistrictCounty": "",
            "cmbDistrictCounty_ClientState": "",
            "cmbBranch": "",
            "cmbBranch_ClientState": "",
            "cmbFilingYear": f"{year}",
            "cmbFilingYear_ClientState": (
                f"{{"
                f'"logEntries":[],'
                f'"value":"{year}",'
                f'"text":"{year}",'
                f'"enabled":true,'
                f'"checkedIndices":[],'
                f'"checkedItemsTextOverflows":false'
                f"}}"
            ),
            "cmbFilingCalenderName": "All Filing Periods",
            "cmbFilingCalenderName_ClientState": (
                "{"
                '"logEntries":[],'
                '"value":"",'
                '"text":"All Filing Periods",'
                '"enabled":true,'
                '"checkedIndices":[],'
                '"checkedItemsTextOverflows":false'
                "}"
            ),
            "dtpFromDate": "",
            "dtpFromDate$dateInput": "",
            "dtpFromDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpFromDate_calendar_SD": "[]",
            "dtpFromDate_calendar_AD": ("[[1980,1,1],[2099,12,30],[2025,4,9]]"),
            "dtpFromDate_ClientState": "",
            "dtpToDate": "",
            "dtpToDate$dateInput": "",
            "dtpToDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpToDate_calendar_SD": "[]",
            "dtpToDate_calendar_AD": ("[[1,1,1],[2099,12,30],[2025,4,9]]"),
            "dtpToDate_ClientState": (
                '{"minDateStr":"1-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00"}'
            ),
            "txtStartAmt": "",
            "txtStartAmt_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minValue":-70368744177664,'
                '"maxValue":70368744177664,'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "txtEndAmt": "",
            "txtEndAmt_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minValue":-70368744177664,'
                '"maxValue":70368744177664,'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "cmbExpenseCategory": "",
            "cmbExpenseCategory_ClientState": "",
            "dtpCommunicationFrmDate": "",
            "dtpCommunicationFrmDate$dateInput": "",
            "dtpCommunicationFrmDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1980-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpCommunicationFrmDate_calendar_SD": "[]",
            "dtpCommunicationFrmDate_calendar_AD": (
                "[[1980,1,1],[2099,12,30],[2025,4,9]]"
            ),
            "dtpCommunicationFrmDate_ClientState": "",
            "dtpCommunicationToDate": "",
            "dtpCommunicationToDate$dateInput": "",
            "dtpCommunicationToDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpCommunicationToDate_calendar_SD": "[]",
            "dtpCommunicationToDate_calendar_AD": "[[1,1,1],[2099,12,30],[2025,4,9]]",
            "dtpCommunicationToDate_ClientState": (
                '{"minDateStr":"1-01-01-00-00-00","maxDateStr":"2099-12-31-00-00-00"}'
            ),
            "UserListDialog_ClientState": "",
            "RadWindowManager1_ClientState": "",
            "__ASYNCPOST": "true",
            "RadAJAXControlID": "cfis_ajxPanel",
        }

        session.headers.update(
            {
                "X-MicrosoftAjax": "Delta=true",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": WI_EXPENSE_TRANSACTIONS_URL,
                "Origin": "https://cfis.wi.gov",
            }
        )

        # Select year
        session.post(WI_EXPENSE_TRANSACTIONS_URL, data=payload)

        # Search
        search_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|btnSearch",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "btnSearch.x": "39",
            "btnSearch.y": "11",
        }
        session.post(WI_EXPENSE_TRANSACTIONS_URL, data=search_payload)

        # Download CSV
        csv_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|cmdText",
            "__EVENTTARGET": "cmdText",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
        res_csv = session.post(
            WI_EXPENSE_TRANSACTIONS_URL, data=csv_payload, stream=True
        )
        logger.info(f"CSV POST status for {year}: {res_csv.status_code}")

        if "attachment" in res_csv.headers.get("content-disposition", ""):
            data_path = Path(WI_DATA_PATH_PREFIX) / "expense_transactions"
            data_path.mkdir(parents=True, exist_ok=True)
            file_path = data_path / f"{year}_expense_transactions.csv"
            with open(file_path, "wb") as f:
                f.write(res_csv.content)
            logger.info(f"✅ CSV saved to {file_path}")
        else:
            logger.warning(f"❌ Failed to download CSV for year {year}")


@dg.asset(deps=["wi_expense_transactions_get_final_html"])
def wi_expense_transactions_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Wisconsin receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_expense_transactions.csv` files inside the
        `expense_transactions/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return wi_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="wi_expense_transactions_landing",
        table_columns_name=[
            "registrant_name",
            "ethcfid",
            "branch",
            "payee_name",
            "transaction_date",
            "communication_date",
            "expense_purpose",
            "expense_category",
            "filing_period_name",
            "filing_fee_name",
            "recount_name",
            "recall_name",
            "referendum_name",
            "ind_exp_candidate_name",
            "support_oppose",
            "amount",
            "comment",
            "reports_72_hr",
            "payee_address_line1",
            "payee_address_line2",
            "payee_city",
            "payee_state",
            "zip",
            "segregated_fund_flag",
            "empty_column",
        ],
        data_validation_callback=lambda row: len(row) == 25,
        category="expense_transactions",
    )


# -------------------------------
# Dagster Assets For Registered Committess
# -------------------------------


@dg.asset
def wi_registered_committees_get_session() -> dict:
    """
    Initializes a session and retrieves the initial session cookies and headers
    from the Wisconsin Public Registration Filed Reports page.

    Returns:
        dict: Dictionary containing `cookies` and `headers` for the session.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()

    session.get(WI_REGISTERED_COMMITTEES_REGISTRATION_URL)

    res = session.get(f"{WI_BASE_URL}/Public/PublicNote.aspx?Page=RegistrantList")
    logger.info(f"Initial GET status: {res.status_code}")

    return {"cookies": session.cookies.get_dict(), "headers": dict(session.headers)}


@dg.asset(deps=["wi_registered_committees_get_session"])
def wi_registered_committees_get_intermediate_note(
    wi_registered_committees_get_session: dict,
) -> dict:
    """
    Performs a follow-up GET request to the intermediate 'note' page required
    before accessing filed reports. Extracts updated cookies, headers, and
    necessary hidden form fields (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`).

    Args:
        wi_registered_committees_get_session (dict): Session metadata from the
        initial GET.

    Returns:
        dict: Updated session metadata and extracted hidden form field values.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_registered_committees_get_session["cookies"])

    res = session.get(WI_REGISTERED_COMMITTEES_PUBLIC_NOTE_URL)
    logger.info(f"Note page GET status: {res.status_code}")

    html = res.text
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
        "viewstate": wi_extract_hidden_input(html, "__VIEWSTATE"),
        "viewstategenerator": wi_extract_hidden_input(html, "__VIEWSTATEGENERATOR"),
    }


@dg.asset(deps=["wi_registered_committees_get_intermediate_note"])
def wi_registered_committees_get_post_continue(
    wi_registered_committees_get_intermediate_note: dict,
):
    """
    Submits a POST request to continue past the intermediate note page, simulating
    the user clicking the "Continue" button. This step is necessary to unlock
    access to the actual report search page.

    Args:
        wi_registered_committees_get_intermediate_note (dict):
        Metadata including cookies,
        headers, and hidden form fields from the note page.

    Returns:
        dict: Updated session metadata after the POST request.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_registered_committees_get_intermediate_note["cookies"])
    session.headers.update(wi_registered_committees_get_intermediate_note["headers"])

    res = session.post(
        WI_REGISTERED_COMMITTEES_PUBLIC_NOTE_URL,
        data={
            "__LASTFOCUS": "",
            "__VIEWSTATE": wi_registered_committees_get_intermediate_note["viewstate"],
            "__VIEWSTATEGENERATOR": (
                wi_registered_committees_get_intermediate_note["viewstategenerator"]
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "btnContinue.x": "55",
            "btnContinue.y": "19",
        },
    )

    logger.info(f"POST continue status: {res.status_code}")
    return {
        "cookies": session.cookies.get_dict(),
        "headers": dict(session.headers),
    }


@dg.asset(deps=["wi_registered_committees_get_post_continue"])
def wi_registered_committees_get_final_html(
    wi_registered_committees_get_post_continue: dict,
):
    """
    Accesses the final Filed Reports page, extracts available filing years,
    simulates selecting each year, performs a search, and then downloads the
    corresponding CSV file for each year.

    Args:
        wi_registered_committees_get_post_continue (dict): Session cookies and headers
        after continuing from the note page

    Returns:
        str: Placeholder string (optionally could return a list of file paths
        or summary if needed); main purpose is side-effect of downloading CSVs.
    """
    logger = dg.get_dagster_logger()
    session = wi_create_session()
    session.cookies.update(wi_registered_committees_get_post_continue["cookies"])
    session.headers.update(wi_registered_committees_get_post_continue["headers"])

    res = session.get(WI_REGISTERED_COMMITTEES_URL)
    logger.info(f"Final GET status: {res.status_code}")

    html_content = res.text

    registrant_types = wi_extract_registrant_types(html_content)
    script_src = wi_extract_radscriptmanager_script(html_content)
    viewstate = wi_extract_hidden_input(html_content, "__VIEWSTATE")
    viewstategenerator = wi_extract_hidden_input(html_content, "__VIEWSTATEGENERATOR")

    logger.info(f"viewstategenerator: {viewstategenerator}")
    logger.info(f"registrant_types: {registrant_types}")

    for registrant_type in registrant_types:
        registrant_index = registrant_type.index(registrant_type) + 1
        payload = {
            "RadScriptManager1": "cfis_ajxPanelPanel|btnSearch",
            "RadScriptManager1_TSM": script_src,
            "cmbType": registrant_type,
            "cmbType_ClientState": json.dumps(
                {
                    "logEntries": [],
                    "value": registrant_index,
                    "text": registrant_type,
                    "enabled": True,
                    "checkedIndices": [],
                    "checkedItemsTextOverflows": False,
                }
            ),
            "txtCandidateName": "",
            "txtCandidateName_ClientState": json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "lastSetTextBoxValue": "",
                }
            ),
            "cmbElectionDate": "",
            "cmbElectionDate_ClientState": "",
            "cmbOffice": "",
            "cmbOffice_ClientState": "",
            "cmbDistrict": "",
            "cmbDistrict_ClientState": "",
            "cmbBranch": "",
            "cmbBranch_ClientState": "",
            "txtGABID": "",
            "txtGABID_ClientState": json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "lastSetTextBoxValue": "",
                }
            ),
            "cmbRegistrantStatus": "",
            "cmbRegistrantStatus_ClientState": "",
            "dtpFromDate": "",
            "dtpFromDate$dateInput": "",
            "dtpFromDate_dateInput_ClientState": json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "minDateStr": "1960-01-01-00-00-00",
                    "maxDateStr": "2099-12-31-00-00-00",
                    "lastSetTextBoxValue": "",
                }
            ),
            "dtpFromDate_calendar_SD": "[]",
            "dtpFromDate_calendar_AD": "[[1960,1,1],[2099,12,30],[2025,4,9]]",
            "dtpFromDate_ClientState": json.dumps(
                {
                    "minDateStr": "1960-01-01-00-00-00",
                    "maxDateStr": "2099-12-31-00-00-00",
                }
            ),
            "dtpToDate": "",
            "dtpToDate$dateInput": "",
            "dtpToDate_dateInput_ClientState": json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "minDateStr": "1960-01-01-00-00-00",
                    "maxDateStr": "2099-12-31-00-00-00",
                    "lastSetTextBoxValue": "",
                }
            ),
            "dtpToDate_calendar_SD": "[]",
            "dtpToDate_calendar_AD": "[[1960,1,1],[2099,12,30],[2025,4,9]]",
            "dtpToDate_ClientState": json.dumps(
                {
                    "minDateStr": "1960-01-01-00-00-00",
                    "maxDateStr": "2099-12-31-00-00-00",
                }
            ),
            "__LASTFOCUS": "",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "__ASYNCPOST": "true",
            "btnSearch.x": "30",
            "btnSearch.y": "14",
            "RadAJAXControlID": "cfis_ajxPanel",
        }

        session.headers.update(
            {
                "X-MicrosoftAjax": "Delta=true",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer": WI_REGISTERED_COMMITTEES_URL,
                "Origin": "https://cfis.wi.gov",
            }
        )

        # Select type
        session.post(WI_REGISTERED_COMMITTEES_URL, data=payload)

        # Search
        search_payload = {
            **payload,
            "RadScriptManager1": "cfis_ajxPanelPanel|btnSearch",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
            "btnSearch.x": "39",
            "btnSearch.y": "11",
        }

        session.post(WI_FILED_REPORTS_URL, data=search_payload, stream=True)

        viewstate = wi_extract_hidden_input(html_content, "__VIEWSTATE")
        viewstategenerator = wi_extract_hidden_input(
            html_content, "__VIEWSTATEGENERATOR"
        )
        script_src = wi_extract_radscriptmanager_script(html_content)

        csv_payload = {
            "RadScriptManager1_TSM": script_src,
            "cmbType": "State Candidate",
            "cmbType_ClientState": "",
            "txtCandidateName": "",
            "txtCandidateName_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "cmbElectionDate": "",
            "cmbElectionDate_ClientState": "",
            "cmbOffice": "",
            "cmbOffice_ClientState": "",
            "cmbDistrict": "",
            "cmbDistrict_ClientState": "",
            "cmbBranch": "",
            "cmbBranch_ClientState": "",
            "txtGABID": "",
            "txtGABID_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "cmbRegistrantStatus": "",
            "cmbRegistrantStatus_ClientState": "",
            "dtpFromDate": "",
            "dtpFromDate$dateInput": "",
            "dtpFromDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1960-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpFromDate_calendar_SD": "[]",
            "dtpFromDate_calendar_AD": ("[[1960,1,1],[2099,12,30],[2025,4,9]]"),
            "dtpFromDate_ClientState": (
                "{"
                '"minDateStr":"1960-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00"'
                "}"
            ),
            "dtpToDate": "",
            "dtpToDate$dateInput": "",
            "dtpToDate_dateInput_ClientState": (
                "{"
                '"enabled":true,'
                '"emptyMessage":"",'
                '"validationText":"",'
                '"valueAsString":"",'
                '"minDateStr":"1960-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00",'
                '"lastSetTextBoxValue":""'
                "}"
            ),
            "dtpToDate_calendar_SD": "[]",
            "dtpToDate_calendar_AD": ("[[1960,1,1],[2099,12,30],[2025,4,9]]"),
            "dtpToDate_ClientState": (
                "{"
                '"minDateStr":"1960-01-01-00-00-00",'
                '"maxDateStr":"2099-12-31-00-00-00"'
                "}"
            ),
            "grdRegistrant_ClientState": "",
            "__LASTFOCUS": "",
            "__EVENTTARGET": "cmdText",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
        }

        res_csv = session.post(
            WI_REGISTERED_COMMITTEES_URL, data=csv_payload, stream=True
        )
        logger.info(f"CSV POST status for {registrant_type}: {res_csv.status_code}")

        if "attachment" in res_csv.headers.get("content-disposition", ""):
            data_path = Path(WI_DATA_PATH_PREFIX) / "registered_committees"
            data_path.mkdir(parents=True, exist_ok=True)
            file_path = data_path / f"{registrant_type}_registered_committees.csv"
            with open(file_path, "wb") as f:
                f.write(res_csv.content)
            logger.info(f"✅ CSV saved to {file_path}")
        else:
            logger.warning(
                f"❌ Failed to download CSV for\
                           registrant_type {registrant_type}"
            )


@dg.asset(deps=["wi_registered_committees_get_final_html"])
def wi_registered_committees_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Wisconsin receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_registered_committees.csv` files inside the
        `registered_committees/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return wi_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="wi_registered_committees_landing",
        table_columns_name=[
            "registrant_type",
            "committee_id",
            "committee_name",
            "committee_address",
            "registrant_status",
            "election_date",
            "branch",
            "registered_date",
            "amended_date",
            "out_state",
            "referendum",
            "independent",
            "exempt",
            "termination_requested_date",
            "empty_column",
        ],
        data_validation_callback=lambda row: len(row) == 15,
        category="registered_committees",
    )

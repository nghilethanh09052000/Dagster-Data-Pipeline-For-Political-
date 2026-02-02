"""
These assets crawl the Connecticut campaign finance data website to
fetch committee information.
The flow follows a specific pattern:
1. Initial GET request to load the search form
2. POST request to submit search parameters
3. Process search results and handle pagination using Next button
"""

import csv
import glob
import os
import random
import re
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup, Tag
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants for Connecticut
CT_DATA_PATH = "./states/connecticut"
CT_BASE_URL = "https://seec.ct.gov/eCrisReporting"
CT_COMMITTEE_SEARCH_URL = f"{CT_BASE_URL}/SearchingCommittee.aspx"
CT_CONTRIBUTION_SEARCH_URL = f"{CT_BASE_URL}/SearchingContribution.aspx"

# Define letters to process
LETTERS = list("abcdefghijklmnopqrstuvwxyz")

# Default timeout for all HTTP requests (connect timeout, read timeout)
# Typical page load >15s; give ample headroom for slow server responses
CT_REQUEST_TIMEOUT = (10, 120)  # 2 minutes for read timeout

# Define columns for committees
CT_COMMITTEE_COLUMNS = [
    "committee_name",
    "candidate_chairperson",
    "treasurer",
    "deputy_treasurer",
    "office_sought",
    "committee_type",
    "termination_date",
    "first_registration_date",
    "party_designation",
    "district",
    "city",
    "state",
]

# Define columns for contributions
CT_CONTRIBUTION_COLUMNS = [
    "root_contrib_id",
    "root_cef_grant_id",
    "receipt_id",
    "committee_name",
    "received_from",
    "city",
    "state",
    "district",
    "office_sought",
    "employer",
    "receipt_type",
    "committee_type",
    "transaction_date",
    "file_to_state_date",
    "amount",
    "receipt_status",
    "occupation",
    "election_year",
    "is_contractor_executive_branch",
    "is_contractor_legislative_branch",
    "is_contractor",
    "is_lobbyist_or_rel",
    "data_source",
    "refile",
    "root_anonymous_id",
    "root_loan_received_id",
    "root_reimbursement_id",
    "root_non_money_org_expend_id",
    "committee_party_affiliation",
    "method_of_payment",
]


"""
Utils
"""


def ct_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
    truncate_query: sql.Composed | None = None,
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

    base_path = Path(CT_DATA_PATH)
    data_files = []

    glob_path = base_path / category

    if glob_path.exists():
        files = glob.glob(str(glob_path / f"*_{category}.csv"))
        data_files.extend(files)

    logger.info(f"Category: {category} | Found {len(data_files)} files.")

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name}.")
        pg_cursor.execute(
            query=(
                truncate_query
                if truncate_query is not None
                else get_sql_truncate_query(table_name=table_name)
            )
        )

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


def ct_get_headers(session: requests.Session, referer: str) -> dict[str, str]:
    """Get headers with session cookies."""
    return {
        "accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;"
            "q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "connection": "keep-alive",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": CT_BASE_URL,
        "referer": referer,
        "sec-ch-ua": (
            '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'
        ),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        ),
        "cookie": "; ".join([f"{k}={v}" for k, v in session.cookies.items()]),
    }


def ct_extract_form_data(html_content: str) -> dict[str, str]:
    """Extract __VIEWSTATE, __VIEWSTATEGENERATOR, and __EVENTVALIDATION from HTML."""
    soup = BeautifulSoup(html_content, "html.parser")

    def get_input_value(input_id: str) -> str:
        element = soup.find("input", {"id": input_id})
        if isinstance(element, Tag):
            return str(element.get("value", ""))
        return ""

    view_state = get_input_value("__VIEWSTATE")
    view_state_generator = get_input_value("__VIEWSTATEGENERATOR")
    event_validation = get_input_value("__EVENTVALIDATION")

    if not view_state:
        match = re.search(r"__VIEWSTATE\|(.*?)\|8\|hiddenField\|", html_content)
        view_state = match.group(1) if match else ""

    if not view_state_generator:
        match = re.search(r"__VIEWSTATEGENERATOR\|([A-Za-z0-9]+)\|", html_content)
        view_state_generator = match.group(1) if match else ""

    return {
        "__VIEWSTATE": view_state,
        "__VIEWSTATEGENERATOR": view_state_generator,
        "__EVENTVALIDATION": event_validation,
    }


def ct_get_committee_search_payload(
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    committee_name: str = "",
    committee_type: str = "All",
    candidate: str = "",
    treasurer: str = "",
    city: str = "",
    state: str = "0",
    office_sought: str = "0",
    party_designation: str = "0",
    committee_status: str = "2",
    no_of_records: str = "25",
) -> dict[str, str]:
    """Generate payload for committee search POST request.

    Args:
        viewstate (str): The VIEWSTATE value from the form.
        viewstate_generator (str): The VIEWSTATEGENERATOR value from the form.
        event_validation (str): The EVENTVALIDATION value from the form.
        committee_name (str, optional): Committee name to search for.
        committee_type (str, optional): Type of committee to search for.
        candidate (str, optional): Candidate name to search for.
        treasurer (str, optional): Treasurer name to search for.
        city (str, optional): City to search for.
        state (str, optional): State to search for.
        office_sought (str, optional): Office sought to search for.
        party_designation (str, optional): Party designation to search for.
        committee_status (str, optional): Committee status to search for.
        no_of_records (str, optional): Number of records to return.

    Returns:
        Dict[str, str]: The payload dictionary for the POST request.
    """
    payload = {
        "ctl00$ScriptManager1": (
            "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$UpdatePanelForCommitteeControl|"
            "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$cmdSearch"
        ),
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__VIEWSTATEENCRYPTED": "",
        "ctl00$ucSearchMenu1$rblPageRange": "C",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCommitteeName": (
            committee_name
        ),
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$rblCommitteeType": (
            committee_type
        ),
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCandidate": candidate,
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtTreasurer": treasurer,
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCity": city,
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlState": state,
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlOfficeSought": (
            office_sought
        ),
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlPartyDesignation": (
            party_designation
        ),
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$lstCommitteeStatus": (
            committee_status
        ),
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlNoOfRecords": (
            no_of_records
        ),
        "__ASYNCPOST": "true",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$cmdSearch": "Search",
    }

    return payload


def ct_get_next_page_payload(
    viewstate: str, viewstate_generator: str, current_page: int
) -> dict[str, str]:
    """Generate payload for Next button POST request."""
    payload = {
        "ctl00$ScriptManager1": (
            "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$UpdatePanelForCommitteeControl|"
            "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$cmdNext"
        ),
        "ctl00$ucSearchMenu1$rblPageRange": "C",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlGoToPage": (
            str(current_page)
        ),
        "hiddenInputToUpdateATBuffer_CommonToolkitScripts": "1",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__VIEWSTATEENCRYPTED": "",
        "__ASYNCPOST": "true",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$cmdNext.x": "1116",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$cmdNext.y": "144",
    }
    return payload


def ct_parse_committee_data(html_content: str) -> list[dict[str, str]]:
    """Parse committee data from HTML table."""
    soup = BeautifulSoup(html_content, "html.parser")
    committees = []

    # Find all table rows with committee data
    rows = soup.find_all("tr", {"onmouseover": "this.style.cursor='hand';"})

    for row in rows:
        if not isinstance(row, Tag):
            continue

        cells = row.find_all("td")
        if len(cells) >= 13:  # Ensure we have all expected columns
            committee = {
                "committee_name": cells[1].get_text(strip=True),
                "candidate_chairperson": cells[2].get_text(strip=True),
                "treasurer": cells[3].get_text(strip=True),
                "deputy_treasurer": cells[4].get_text(strip=True),
                "office_sought": cells[5].get_text(strip=True),
                "committee_type": cells[6].get_text(strip=True),
                "termination_date": cells[7].get_text(strip=True),
                "first_registration_date": cells[8].get_text(strip=True),
                "party_designation": cells[9].get_text(strip=True),
                "district": cells[10].get_text(strip=True),
                "city": cells[11].get_text(strip=True),
                "state": cells[12].get_text(strip=True),
            }
            committees.append(committee)

    return committees


def ct_is_next_button_disabled(html_content: str, name: str) -> bool:
    """Check if the Next button is disabled."""
    soup = BeautifulSoup(html_content, "html.parser")
    next_button = soup.find("input", {"id": name})
    if isinstance(next_button, Tag):
        return bool(next_button.has_attr("disabled"))
    return False


def ct_get_contribution_search_payload(
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    state: str = "",
    start_date: str = "",
    end_date: str = "",
    committee_type: str = "",
    contributor_name: str = "",
    city: str = "",
    office_sought: str = "",
    employer_name: str = "",
    transaction_start_date: str = "",
    transaction_end_date: str = "",
    min_amount: str = "",
    max_amount: str = "",
    committee_name: str = "",
    method_of_payment: str = "",
    sort_by: str = "",
    show_history: str = "0",
    no_of_records: str = "100",
) -> dict[str, str]:
    """Generate payload for contribution search POST request."""
    payload = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__VIEWSTATEENCRYPTED": "",
        "ctl00$ucSearchMenu1$rblPageRange": "C",
        "ctl00$ContentPlaceHolder1$chkAll": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$0": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$1": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$2": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$3": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$4": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$5": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$6": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$7": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$8": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$9": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$10": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$11": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$12": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$13": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$14": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$15": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$16": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$17": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$18": "on",
        "ctl00$ContentPlaceHolder1$chkContributionType$19": "on",
        "ctl00$ContentPlaceHolder1$txtContributorName": "",
        "ctl00$ContentPlaceHolder1$txtCity": "",
        "ctl00$ContentPlaceHolder1$ddlState": "",
        "ctl00$ContentPlaceHolder1$ddlOfficeSought": "",
        "ctl00$ContentPlaceHolder1$txtEmployerName": "",
        "ctl00$ContentPlaceHolder1$txtTransactionStartDate": start_date,
        "ctl00$ContentPlaceHolder1$txtTransactionEndDate": end_date,
        "ctl00$ContentPlaceHolder1$txtMinAmount": "",
        "ctl00$ContentPlaceHolder1$txtMaxAmount": "",
        "ctl00$ContentPlaceHolder1$txtCommittee": "",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCommitteeName": "",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$rblCommitteeType": "All",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCandidate": "",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtTreasurer": "",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$txtCity": "",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlState": "0",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlOfficeSought": "0",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlPartyDesignation": "0",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$lstCommitteeStatus": "2",
        "ctl00$ContentPlaceHolder1$UcCommitteeAdvanceSearch1$ddlNoOfRecords": "25",
        "ctl00$ContentPlaceHolder1$rblCommitteeType": committee_type,
        "ctl00$ContentPlaceHolder1$ddlMethodOfPayment": "",
        "ctl00$ContentPlaceHolder1$ddlSortBy": "",
        "ctl00$ContentPlaceHolder1$rblShowHistory": "0",
        "ctl00$ContentPlaceHolder1$lstNoOfRecords": "1000",
        "ctl00$ContentPlaceHolder1$btnSearch": "Search",
        "hiddenInputToUpdateATBuffer_CommonToolkitScripts": "1",
    }
    return payload


def ct_parse_contribution_data(html_content: str):
    """Parse Connecticut contribution data from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    contributions = []

    table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_gvSearchResult"})
    if not isinstance(table, Tag):
        return contributions

    for row in table.find_all("tr"):
        if not isinstance(row, Tag) or row.find("th"):
            continue

        cells = row.find_all("td")
        if len(cells) < 30 or not all(isinstance(cell, Tag) for cell in cells[:30]):
            continue

        committee_name = ""
        cell = cells[3]

        if isinstance(cell, Tag):
            committee_a_tag = cell.find("a")
            if isinstance(committee_a_tag, Tag):
                committee_name = "".join(
                    span.get_text()
                    for span in committee_a_tag.find_all("span", class_="highlight")
                    if isinstance(span, Tag)
                )

        contribution = {
            "root_contrib_id": cells[0].get_text(strip=True),
            "root_cef_grant_id": cells[1].get_text(strip=True),
            "receipt_id": cells[2].get_text(strip=True),
            "committee_name": committee_name,
            "received_from": cells[4].get_text(strip=True),
            "city": cells[5].get_text(strip=True),
            "state": cells[6].get_text(strip=True),
            "district": cells[7].get_text(strip=True),
            "office_sought": cells[8].get_text(strip=True),
            "employer": cells[9].get_text(strip=True),
            "receipt_type": cells[10].get_text(strip=True),
            "committee_type": cells[11].get_text(strip=True),
            "transaction_date": cells[12].get_text(strip=True),
            "file_to_state_date": cells[13].get_text(strip=True),
            "amount": cells[14].get_text(strip=True),
            "receipt_status": cells[15].get_text(strip=True),
            "occupation": cells[16].get_text(strip=True),
            "election_year": cells[17].get_text(strip=True),
            "is_contractor_executive_branch": cells[18].get_text(strip=True),
            "is_contractor_legislative_branch": cells[19].get_text(strip=True),
            "is_contractor": cells[20].get_text(strip=True),
            "is_lobbyist_or_rel": cells[21].get_text(strip=True),
            "data_source": cells[22].get_text(strip=True),
            "refile": cells[23].get_text(strip=True),
            "root_anonymous_id": cells[24].get_text(strip=True),
            "root_loan_received_id": cells[25].get_text(strip=True),
            "root_reimbursement_id": cells[26].get_text(strip=True),
            "root_non_money_org_expend_id": cells[27].get_text(strip=True),
            "committee_party_affiliation": cells[28].get_text(strip=True),
            "method_of_payment": cells[29].get_text(strip=True),
        }

        contributions.append(contribution)

    return contributions


def ct_extract_states(html_content: str) -> list[str]:
    """Extract states from the state dropdown menu in the HTML."""
    soup = BeautifulSoup(html_content, "html.parser")
    states = []

    # Find the state dropdown menu
    state_select = soup.find("select", {"id": "ctl00_ContentPlaceHolder1_ddlState"})
    if isinstance(state_select, Tag):
        # Get all option values except the empty one
        for option in state_select.find_all("option"):
            if isinstance(option, Tag):
                state_value = option.get("value")
                if state_value:  # Skip empty values
                    states.append(state_value)

    return states


def ct_get_next_page_contributions_payload(
    viewstate: str,
    viewstate_generator: str,
    current_page: int,
    total_pages: int = 1,
) -> dict[str, str]:
    """Generate payload for contribution next page POST request.

    Args:
        viewstate (str): The VIEWSTATE value from the form
        viewstate_generator (str): The VIEWSTATEGENERATOR value from the form
        current_page (int): The current page number
        total_pages (int): The total number of pages

    Returns:
        dict[str, str]: The payload for the next page request
    """
    return {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__VIEWSTATEENCRYPTED": "",
        "ctl00$ucSearchMenu1$rblPageRange": "C",
        "ctl00$ContentPlaceHolder1$cmdNext.x": "1341",
        "ctl00$ContentPlaceHolder1$cmdNext.y": "132",
        "ctl00$ContentPlaceHolder1$ddlTotofPages": str(total_pages),
    }


def ct_fetch_contributions_data(
    type: str, committee_type: str, start_date: str, end_date: str
):
    """Fetch all contribution data."""
    logger = dg.get_dagster_logger(f"ct_fetch_contributions_data_{type}")
    results = []

    # Create output directory
    output_dir = Path(f"{CT_DATA_PATH}/contributions")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create session and get form data
    session = requests.Session()
    logger.info(f"Making GET request to {CT_CONTRIBUTION_SEARCH_URL}")
    response = session.get(
        CT_CONTRIBUTION_SEARCH_URL,
        headers=ct_get_headers(session, CT_CONTRIBUTION_SEARCH_URL),
        timeout=CT_REQUEST_TIMEOUT,
    )
    logger.info(f"GET response status: {response.status_code}")

    logger.info(f"Processing start_date: {start_date} with type {committee_type}")

    # Get form data for this state
    form_data = ct_extract_form_data(response.text)

    # Submit search form for the state
    payload = ct_get_contribution_search_payload(
        viewstate=form_data["__VIEWSTATE"],
        viewstate_generator=form_data["__VIEWSTATEGENERATOR"],
        event_validation=form_data["__EVENTVALIDATION"],
        start_date=start_date,
        end_date=end_date,
        committee_type=committee_type,
    )

    logger.info(f"Making POST request to {CT_CONTRIBUTION_SEARCH_URL}")
    response = session.post(
        CT_CONTRIBUTION_SEARCH_URL,
        data=payload,
        headers=ct_get_headers(session, CT_CONTRIBUTION_SEARCH_URL),
        timeout=CT_REQUEST_TIMEOUT,
    )
    logger.info(f"POST response status: {response.status_code}")

    # Extract form data and contribution data from first page
    current_form_data = ct_extract_form_data(response.text)
    contributions = ct_parse_contribution_data(response.text)

    if contributions:
        results.extend(contributions)
        logger.info(
            f"Type {committee_type} Found {len(contributions)}\
                contributions on page 1 for start_date {start_date}"
        )

    page = 1
    while True:
        if ct_is_next_button_disabled(
            response.text, "ctl00_ContentPlaceHolder1_cmdNext"
        ):
            logger.info(f"No more pages for start_date {start_date}")
            break

        next_page_payload = ct_get_next_page_contributions_payload(
            viewstate=current_form_data["__VIEWSTATE"],
            viewstate_generator=current_form_data["__VIEWSTATEGENERATOR"],
            current_page=page,
        )

        response = session.post(
            CT_CONTRIBUTION_SEARCH_URL,
            data=next_page_payload,
            headers=ct_get_headers(session, CT_CONTRIBUTION_SEARCH_URL),
            timeout=CT_REQUEST_TIMEOUT,
        )

        current_form_data = ct_extract_form_data(response.text)
        contributions = ct_parse_contribution_data(response.text)

        if not contributions:
            logger.warning(
                f"No contributions found on page {page + 1}\
                            for start_date {start_date}"
            )
            break

        results.extend(contributions)
        logger.info(
            f"Found {len(contributions)} contributions on page "
            f"{page + 1} for start_date {start_date} with"
            f"type {committee_type}"
        )

        time.sleep(0.5 + random.uniform(0.5, 1))

        page += 1

    # Save results for this year
    if results:
        format_start_date = datetime.strptime(start_date, "%m/%d/%Y").strftime(
            "%Y_%m_%d"
        )
        csv_path = output_dir / f"type_{type}_{format_start_date}_contributions.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CT_CONTRIBUTION_COLUMNS)
            writer.writeheader()
            writer.writerows(results)
        logger.info(
            f"Saved {len(results)} contributions for start_date\
            {start_date} to {csv_path} with type {committee_type}"
        )
        results.extend(results)


"""
Committees Fetching
"""


def ct_process_letter_page(
    letter: str, committee_type: str, type: str
) -> list[dict[str, str]]:
    """Process a single page for a letter and return its committees."""
    logger = dg.get_dagster_logger()

    # Create output directory
    output_dir = Path(f"{CT_DATA_PATH}/committees")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create session and get form data
    session = requests.Session()
    response = session.get(
        CT_COMMITTEE_SEARCH_URL,
        headers=ct_get_headers(session, CT_COMMITTEE_SEARCH_URL),
        timeout=CT_REQUEST_TIMEOUT,
    )
    form_data = ct_extract_form_data(response.text)

    # Initialize results list
    results = []

    # Submit search form for the letter
    payload = ct_get_committee_search_payload(
        viewstate=form_data["__VIEWSTATE"],
        viewstate_generator=form_data["__VIEWSTATEGENERATOR"],
        event_validation=form_data["__EVENTVALIDATION"],
        committee_name=letter,
        committee_type=committee_type,
    )

    response = session.post(
        CT_COMMITTEE_SEARCH_URL,
        data=payload,
        headers=ct_get_headers(session, CT_COMMITTEE_SEARCH_URL),
        timeout=CT_REQUEST_TIMEOUT,
    )

    # Extract form data and committee data from first page
    current_form_data = ct_extract_form_data(response.text)
    committees = ct_parse_committee_data(response.text)

    if committees:
        results.extend(committees)
        logger.info(
            f"Found {len(committees)} committees with type {type} \
            on page 1 for letter {letter}"
        )

    # Process remaining pages

    page = 1
    while True:
        if ct_is_next_button_disabled(
            response.text, "ctl00_ContentPlaceHolder1_UcCommitteeAdvanceSearch1_cmdNext"
        ):
            logger.info(f"No more pages for letter {letter}")
            break

        payload = ct_get_next_page_payload(
            viewstate=current_form_data["__VIEWSTATE"],
            viewstate_generator=current_form_data["__VIEWSTATEGENERATOR"],
            current_page=page,
        )

        response = session.post(
            CT_COMMITTEE_SEARCH_URL,
            data=payload,
            headers=ct_get_headers(session, CT_COMMITTEE_SEARCH_URL),
            timeout=CT_REQUEST_TIMEOUT,
        )

        current_form_data = ct_extract_form_data(response.text)
        committees = ct_parse_committee_data(response.text)

        if not committees:
            logger.warning(
                f"No committees type {type} found on page\
                {page + 1} for letter {letter}"
            )
            break

        results.extend(committees)
        logger.info(
            f"Found {len(committees)} committees type {type} on page\
            {page + 1} for letter {letter}"
        )

        time.sleep(0.5 + random.uniform(0.5, 1))

        page += 1

    # Save all committees to a single CSV file
    if results:
        csv_path = output_dir / f"type_{type}_letter_{letter}_committees.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CT_COMMITTEE_COLUMNS)
            writer.writeheader()
            writer.writerows(results)
        logger.info(
            f"Saved {len(results)} total committees\
            for type {type} letter {letter}"
        )

    return results


ct_schedule_static_partition = dg.StaticPartitionsDefinition(partition_keys=LETTERS)


@dg.asset(partitions_def=ct_schedule_static_partition, pool="ct_api")
def ct_fetch_committee_candidate_assets(context: dg.AssetExecutionContext):
    """Fetch committee data for candidate committees."""
    letter = context.partition_key
    return ct_process_letter_page(letter=letter, committee_type="1", type="candidate")


@dg.asset(partitions_def=ct_schedule_static_partition, pool="ct_api")
def ct_fetch_committee_exploratoy_assets(context: dg.AssetExecutionContext):
    """Fetch committee data for exploratory committees."""
    letter = context.partition_key
    return ct_process_letter_page(letter=letter, committee_type="4", type="exploratory")


@dg.asset(partitions_def=ct_schedule_static_partition, pool="ct_api")
def ct_fetch_committee_party_assets(context: dg.AssetExecutionContext):
    """Fetch committee data for party committees."""
    letter = context.partition_key
    return ct_process_letter_page(letter=letter, committee_type="2", type="party")


@dg.asset(partitions_def=ct_schedule_static_partition, pool="ct_api")
def ct_fetch_committee_politicial_assets(context: dg.AssetExecutionContext):
    """Fetch committee data for political committees."""
    letter = context.partition_key
    return ct_process_letter_page(letter=letter, committee_type="3", type="political")


"""
Contributions Fetching
"""
ct_schedule_daily_partition = dg.DailyPartitionsDefinition(
    start_date=datetime(2016, 1, 1),
    timezone="America/New_York",
    fmt="%m/%d/%Y",
    end_offset=1,
)


@dg.asset(partitions_def=ct_schedule_daily_partition, pool="ct_api")
def ct_fetch_contributions_candidate_data(context: dg.AssetExecutionContext):
    """Fetch contributions data for party committees."""
    start_date = context.partition_key
    return ct_fetch_contributions_data(
        type="candidate", committee_type="1", start_date=start_date, end_date=start_date
    )


@dg.asset(partitions_def=ct_schedule_daily_partition, pool="ct_api")
def ct_fetch_contributions_exploratory_data(context: dg.AssetExecutionContext):
    """Fetch contributions data for party committees."""
    start_date = context.partition_key
    return ct_fetch_contributions_data(
        type="exploratory",
        committee_type="4",
        start_date=start_date,
        end_date=start_date,
    )


@dg.asset(partitions_def=ct_schedule_daily_partition, pool="ct_api")
def ct_fetch_contributions_party_data(context: dg.AssetExecutionContext):
    """Fetch contributions data for party committees."""
    start_date = context.partition_key
    return ct_fetch_contributions_data(
        type="party", committee_type="2", start_date=start_date, end_date=start_date
    )


@dg.asset(partitions_def=ct_schedule_daily_partition, pool="ct_api")
def ct_fetch_contributions_political_data(context: dg.AssetExecutionContext):
    """Fetch contributions data for political committees."""
    start_date = context.partition_key
    return ct_fetch_contributions_data(
        type="political", committee_type="3", start_date=start_date, end_date=start_date
    )


# Create insert assets
@dg.asset(
    deps=[
        ct_fetch_committee_candidate_assets,
        ct_fetch_committee_exploratoy_assets,
        ct_fetch_committee_party_assets,
        ct_fetch_committee_politicial_assets,
    ],
    partitions_def=ct_schedule_static_partition,
)
def ct_inserting_committees_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert committee data into the landing table."""
    return ct_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ct_committees_landing",
        table_columns_name=CT_COMMITTEE_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(CT_COMMITTEE_COLUMNS)),
        category="committees",
    )


@dg.asset(
    deps=[
        ct_fetch_contributions_candidate_data,
        ct_fetch_contributions_exploratory_data,
        ct_fetch_contributions_political_data,
        ct_fetch_contributions_party_data,
    ],
    partitions_def=ct_schedule_daily_partition,
)
def ct_inserting_contributions_reports_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """Insert contribution data into the landing table."""

    table_name = "ct_contributions_landing"

    table_name_identifier = sql.Identifier(table_name)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE transaction_date = {transaction_date}"
    ).format(table_name=table_name_identifier, transaction_date=context.partition_key)

    return ct_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=CT_CONTRIBUTION_COLUMNS,
        data_validation_callback=(lambda row: len(row) == len(CT_CONTRIBUTION_COLUMNS)),
        category="contributions",
        truncate_query=truncate_query,
    )

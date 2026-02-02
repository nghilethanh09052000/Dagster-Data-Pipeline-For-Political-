import csv
import glob
import html
import os
import random
import re
import time
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

KS_CONTRIBUTION_URL = (
    "https://kssos.org/elections/cfr_viewer/cfr_examiner_contribution.aspx"
)
KS_CONTRIBUTION_RESULTS_URL = (
    "https://kssos.org/elections/cfr_viewer/cfr_examiner_contribution_results.aspx"
)

KS_EXPENDITURE_URL = (
    "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_expenditure.aspx"
)
KS_EXPENDITURE_RESULTS_URL = (
    "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_expenditure_results.aspx"
)

KS_PAC_URL = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx"
KS_PAC_SEARCH_RESULTS_URL = (
    "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_search_results.aspx"
)


KS_CANDIDATE_URL = "https://sos.ks.gov/elections/elections_upcoming_candidate.aspx"
KS_DATA_PATH_PREFIX = "./states/kansas"
HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "sec-ch-ua": ('"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    ),
}

# URLs for PAC data
KS_PAC_ENTRY_URL = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx"
KS_PAC_SEARCH_URL = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx"
KS_PAC_SEARCH_RESULTS_URL = (
    "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_search_results.aspx"
)

# Headers for PAC requests
PAC_SEARCH_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://sos.ks.gov",
    "priority": "u=0, i",
    "referer": "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx",
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
        "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    ),
}


# -------------------------------
# Utilities
# -------------------------------


def ks_extract_hidden_input_value(html: str, name: str) -> str:
    """
    Extracts the value of a hidden <input> tag by name from an HTML form.

    Args:
        html (str): The HTML content to parse.
        name (str): The name attribute of the hidden input field.

    Returns:
        str: The value of the input field, or an empty string if not found.
    """
    tag = BeautifulSoup(html, "html.parser").find("input", {"name": name})
    if isinstance(tag, Tag) and tag.has_attr("value"):
        return str(tag["value"])
    return ""


def ks_get_pagination_state(html_src: str) -> tuple[int, bool]:
    """
    Determines the pagination state of a results table.

    Parses the HTML to find:
    - The highest page number (int).
    - Whether an ellipsis ("...") exists in the pagination row,
    indicating more pages.

    Args:
        html (str): HTML content of a paginated results page.

    Returns:
        tuple[int, bool]: A tuple with:
            - last_page: the highest page number found.
            - has_ellipsis: True if the pagination row includes an ellipsis.
    """
    soup = BeautifulSoup(html_src, "html.parser")

    all_next_page_link = soup.find_all("a", href=re.compile("__doPostBack"))

    last_page = 1
    has_ellipsis = False
    for el in all_next_page_link:
        print(el)
        if not isinstance(el, Tag):
            continue

        next_page_url = el.get("href")
        if html.unescape(el.get_text(strip=True)) == "...":
            has_ellipsis = True

        if next_page_url is None:
            continue

        next_page_matched = re.match(
            r"javascript:[ +]?__doPostBack\('\w+',[ +]?'Page\$([0-9]+)'\)",
            html.unescape(str(next_page_url)),
        )

        if next_page_matched is None:
            continue

        page_num_str = next_page_matched.group(0)
        page_num = int(page_num_str) if page_num_str.isdigit() else 0

        if last_page < page_num:
            last_page = page_num

    return last_page, has_ellipsis


def ks_get_election_values(html: str) -> list[str]:
    """
    Extracts all election values from the 'ddlElections'
    dropdown on the candidate page.

    Args:
        html (str): The HTML of the initial candidate filing form.

    Returns:
        list[str]: A list of string values corresponding to
        each election cycle option.
    """
    soup = BeautifulSoup(html, "html.parser")
    select_tag = soup.find("select", {"name": "ddlElections"})
    if not isinstance(select_tag, Tag):
        return []

    return [
        str(option.get("value"))
        for option in select_tag.find_all("option")
        if isinstance(option, Tag) and option.has_attr("value")
    ]


def ks_extract_offices(html: str) -> list[str]:
    """Extract office types from the select element.

    Args:
        html: HTML content containing the office select element

    Returns:
        List of office values (e.g. ["12", "13"])
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    office_select = soup.find("select", {"name": "drpdownOffice"})

    if not isinstance(office_select, Tag):
        return []

    return [
        str(option.get("value"))
        for option in office_select.find_all("option")
        if isinstance(option, Tag) and option.has_attr("value")
    ]


def ks_write_results_to_csv(
    results: list[dict], page: int | str, output_dir: str, name: str
):
    """
    Writes a list of dictionaries to a CSV file in a structured
    directory.

    Args:
        results (list[dict]): The list of records to write.
        page (int | str): Page number or election identifier
        used in the filename.
        output_dir (str): Directory to save the output.
        name (str): Logical name of the dataset
        (e.g., "contribution", "expenditure").

    Output:
        A CSV file named `page_{page}_{name}.csv` saved in the
        specified output directory.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(output_dir) / f"page_{page}_{name}.csv"
    with file_path.open("w", newline="", encoding="utf-8") as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)


def ks_build_initial_payload(html: str, name: str) -> dict:
    """
    Builds the initial POST payload to submit a form search
    on the Kansas campaign finance site.

    Args:
        html (str): HTML content of the initial form page.
        name (str): The type of search being performed
        ("contribution" or other, e.g., "expenditure").

    Returns:
        dict: A dictionary representing the POST form payload.
    """
    if name == "contribution":
        return {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": (ks_extract_hidden_input_value(html, "__VIEWSTATE")),
            "__VIEWSTATEGENERATOR": (
                ks_extract_hidden_input_value(html, "__VIEWSTATEGENERATOR")
            ),
            "__EVENTVALIDATION": (
                ks_extract_hidden_input_value(html, "__EVENTVALIDATION")
            ),
            "txtContributorName": "",
            "txtCandidateName": "",
            "txtContributorCity": "",
            "ddlStates": "",
            "ddlContributionType": "",
            "txtCashAmount": "",
            "txtStartDate": "01/01/1990",
            "txtEndDate": datetime.today().strftime("%m/%d/%Y"),
            "btnSubmit": "Submit",
        }
    elif name == "expenditure":
        return {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": (ks_extract_hidden_input_value(html, "__VIEWSTATE")),
            "__VIEWSTATEGENERATOR": (
                ks_extract_hidden_input_value(html, "__VIEWSTATEGENERATOR")
            ),
            "__EVENTVALIDATION": (
                ks_extract_hidden_input_value(html, "__EVENTVALIDATION")
            ),
            "txtEntity": "",
            "txtCandidateName": "",
            "txtCity": "",
            "ddlStates": "",
            "ddlExpenditureType": "",
            "txtAmount": "",
            "txtStartDate": "01/01/1990",
            "txtEndDate": datetime.today().strftime("%m/%d/%Y"),
            "btnSubmit": "Submit",
        }
    else:
        return {
            "__EVENTTARGET": "",
            "__VIEWSTATE": (ks_extract_hidden_input_value(html, "__VIEWSTATE")),
            "__VIEWSTATEGENERATOR": (
                ks_extract_hidden_input_value(html, "__VIEWSTATEGENERATOR")
            ),
            "__EVENTVALIDATION": (
                ks_extract_hidden_input_value(html, "__EVENTVALIDATION")
            ),
            "ddlViewerOptions": "PAC",
            "btnSubmit": "Submit",
        }


def ks_build_pagination_payload(html: str, page: int, target: str) -> dict:
    """
    Builds the POST payload required to request a specific page in
    a paginated results table.

    Args:
        html (str): HTML content of the current page to extract viewstate data.
        page (int): Target page number to request.
        target (str): The ID of the table triggering the pagination event
        (e.g., "gvContributionResults").

    Returns:
        dict: A dictionary representing the pagination form payload.
    """
    return {
        "__EVENTTARGET": target,
        "__EVENTARGUMENT": f"Page${page}",
        "__VIEWSTATE": (ks_extract_hidden_input_value(html, "__VIEWSTATE")),
        "__VIEWSTATEGENERATOR": (
            ks_extract_hidden_input_value(html, "__VIEWSTATEGENERATOR")
        ),
        "__EVENTVALIDATION": (ks_extract_hidden_input_value(html, "__EVENTVALIDATION")),
    }


def build_pac_search_payload(html_content: str, office: str) -> dict:
    """Build payload for PAC search request.

    Args:
        html_content: HTML content containing form state
        office: Office value to search for

    Returns:
        Dictionary containing form payload
    """
    return {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": (ks_extract_hidden_input_value(html_content, "__VIEWSTATE")),
        "__VIEWSTATEGENERATOR": (
            ks_extract_hidden_input_value(html_content, "__VIEWSTATEGENERATOR")
        ),
        "__EVENTVALIDATION": (
            ks_extract_hidden_input_value(html_content, "__EVENTVALIDATION")
        ),
        "txtOrganizationName": "",
        "drpdownOffice": str(office),
        "drpdownFilingType": "",
        "txtStartDate": "",
        "txtEndDate": "",
        "btnSearch": "Submit Search",
    }


def get_pac_headers(cookie_string: str) -> dict:
    """Get headers for PAC requests with cookie.

    Args:
        cookie_string: Session cookie string

    Returns:
        Dictionary of headers for PAC requests
    """
    return {**PAC_SEARCH_HEADERS, "cookie": cookie_string}


def ks_insert_raw_file_to_landing_table(
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

    base_path = Path(KS_DATA_PATH_PREFIX)
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
# Assets to fetch data
# -------------------------------


@dg.asset(pool="ks_api")
def ks_get_and_save_contribution_data():
    """
    Scrapes Kansas campaign finance contribution data and saves it to CSV files.

    This asset performs the following steps:
    1. Initializes a session and loads the initial contribution search form.
    2. Submits the form with default values to trigger a search from 1990 to today.
    3. Iteratively paginates through all result pages.
    4. Parses each page for contribution records and writes them to CSV, one
    file per page.

    Parsed fields include:
        - candidate_name
        - contributor_name
        - contributor_address
        - contributor_city
        - contributor_state
        - contributor_zip
        - occupation
        - industry
        - date_received
        - tender_type
        - amount
        - in_kind_amount
        - in_kind_description
        - period_start
        - period_end

    Output:
        CSV files saved in the output directory: {KS_DATA_PATH_PREFIX}/contribution

    Notes:
        - Uses session state to persist viewstate/viewstategenerator across
            form submissions.
        - Pagination stops when the last page is reached (detected via
            ellipsis state).
    """

    def parse_contribution_table(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table#gvContributionResults tr")[1:]
        data = []
        for row in rows:
            spans = row.find_all("span")
            if len(spans) >= 15:
                data.append(
                    {
                        "candidate_name": spans[0].text.strip(),
                        "contributor_name": spans[1].text.strip(),
                        "contributor_address": spans[2].text.strip(),
                        "contributor_city": spans[3].text.strip(),
                        "contributor_state": spans[4].text.strip(),
                        "contributor_zip": spans[5].text.strip(),
                        "occupation": spans[6].text.strip(),
                        "industry": spans[7].text.strip(),
                        "date_received": spans[8].text.strip(),
                        "tender_type": spans[9].text.strip(),
                        "amount": spans[10].text.strip(),
                        "in_kind_amount": spans[11].text.strip(),
                        "in_kind_description": spans[12].text.strip(),
                        "period_start": spans[13].text.strip(),
                        "period_end": spans[14].text.strip(),
                    }
                )
        return data

    logger = dg.get_dagster_logger("ks_get_and_save_contribution_data")
    name = "contribution"
    output_dir = KS_DATA_PATH_PREFIX + "/" + name
    session = requests.Session()

    initial_res = session.get(KS_CONTRIBUTION_URL, headers=HEADERS)
    html_content = initial_res.text

    session.post(
        KS_CONTRIBUTION_URL,
        data=ks_build_initial_payload(html_content, name),
        headers=HEADERS,
    )
    response = session.get(
        KS_CONTRIBUTION_RESULTS_URL,
        headers={**HEADERS},
    )
    html_content = response.text

    page = 1
    while True:
        logger.info(f"Extracting contribution page {page}...")
        data = parse_contribution_table(html_content)
        logger.info(f"Extracted {len(data)} rows on page {page}")
        ks_write_results_to_csv(data, page, output_dir, name)

        last_page, ellipsis = ks_get_pagination_state(html_content)
        if not ellipsis and page >= last_page:
            logger.info("Reached final page — done.")
            break

        page += 1
        payload = ks_build_pagination_payload(
            html_content, page, "gvContributionResults"
        )

        time.sleep(0.5 + random.uniform(0.5, 2))
        response = session.post(
            KS_CONTRIBUTION_RESULTS_URL,
            data=payload,
            headers={**HEADERS},
        )
        html_content = response.text


@dg.asset(pool="ks_api")
def ks_get_and_save_expenditure_data():
    """
    Scrapes Kansas campaign finance expenditure data and saves it to CSV files.

    This asset performs the following steps:
    1. Loads the initial expenditure search form from the Kansas Ethics website.
    2. Submits the form with default values to trigger a full search
    3. Iterates through all result pages and parses each expenditure record.
    4. Saves parsed data into CSV files, one file per page, in a
    structured output directory.

    Extracted fields include:
        - candidate_name
        - recipient
        - address
        - city
        - state
        - zip_code
        - date
        - expenditure_description
        - amount
        - start_date (reporting period)
        - end_date (reporting period)

    Output:
        CSV files saved under: {KS_DATA_PATH_PREFIX}/expediture

    Notes:
        - Maintains ASP.NET session state with VIEWSTATE and related fields.
        - Detects and navigates pagination using the Kansas Ethics postback system.
        - Stops scraping when all pages have been processed (no ellipsis or more pages).
    """

    def parse_expenditure_table(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table#gvExpenditureResults tr")[1:]
        data = []

        for row in rows:
            spans = row.find_all("span")
            if len(spans) >= 11:
                candidate_name = spans[0].get_text(strip=True)
                recipient = spans[1].get_text(strip=True)
                address = spans[2].get_text(strip=True)
                city = spans[3].get_text(strip=True)
                state = spans[4].get_text(strip=True)
                zip_code = spans[5].get_text(strip=True)
                date = spans[6].get_text(strip=True)
                description = spans[7].get_text(strip=True)
                amount = spans[8].get_text(strip=True)
                start_date = spans[9].get_text(strip=True)
                end_date = spans[10].get_text(strip=True)

                data.append(
                    {
                        "candidate_name": candidate_name,
                        "recipient": recipient,
                        "address": address,
                        "city": city,
                        "state": state,
                        "zip_code": zip_code,
                        "date": date,
                        "expenditure_description": description,
                        "amount": amount,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )

        return data

    logger = dg.get_dagster_logger("ks_get_and_save_expenditure_data")
    name = "expenditure"
    output_dir = KS_DATA_PATH_PREFIX + "/" + name
    session = requests.Session()

    initial_res = session.get(KS_EXPENDITURE_URL, headers=HEADERS)
    html_content = initial_res.text

    session.post(
        KS_EXPENDITURE_URL,
        data=ks_build_initial_payload(html_content, name),
        headers=HEADERS,
    )
    cookie_string = "; ".join(
        [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
    )
    response = session.get(
        KS_EXPENDITURE_RESULTS_URL,
        headers={**HEADERS, "cookie": cookie_string},
    )
    html_content = response.text

    page = 1
    while True:
        logger.info(f"Extracting expenditure page {page}...")
        data = parse_expenditure_table(html_content)
        logger.info(f"Extracted {len(data)} rows on page {page}")
        ks_write_results_to_csv(data, page, output_dir, name)

        last_page, ellipsis = ks_get_pagination_state(html_content)
        if not ellipsis and page >= last_page:
            logger.info("Reached final page — done.")
            break

        page += 1
        payload = ks_build_pagination_payload(
            html_content, page, "gvExpenditureResults"
        )

        time.sleep(0.5 + random.uniform(0.5, 2))
        response = session.post(
            KS_EXPENDITURE_RESULTS_URL,
            data=payload,
            headers={**HEADERS, "cookie": cookie_string},
        )
        html_content = response.text


@dg.asset(pool="ks_api")
def ks_get_and_save_party_politicial_committee_data():
    """
    Scrapes Kansas campaign finance pac data and saves it to CSV files.

    This asset performs the following steps:
    1. Loads the initial pac search form from the Kansas Ethics website.
    2. Submits the form with default values to trigger a full search
    3. Iterates through all result pages and parses each pac record.
    4. Saves parsed data into CSV files, one file per page, in a
    structured output directory.

    Output:
        CSV files saved under: {KS_DATA_PATH_PREFIX}/committees
    Notes:
        - Maintains ASP.NET session state with VIEWSTATE and related fields.
        - Detects and navigates pagination using the Kansas Ethics postback system.
        - Stops scraping when all pages have been processed (no ellipsis or more pages).
    """

    def parse_committee_table(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Find all rows in the table
        rows = soup.find_all("tr")
        if not isinstance(rows, list):
            return []

        for row in rows:
            if not isinstance(row, Tag):
                continue

            # Skip pagination rows
            if row.find("table"):
                continue

            # Get all cells in the row
            cells = row.find_all("td")
            if not isinstance(cells, list) or len(cells) < 3:
                continue

            # First cell contains dates
            date_cell = cells[0]
            if not isinstance(date_cell, Tag):
                continue

            date_span = date_cell.find("span", id=re.compile(r"lblDate_"))

            date = date_span.get_text(strip=True) if isinstance(date_span, Tag) else ""

            # Second cell contains committee name
            name_cell = cells[1]
            if not isinstance(name_cell, Tag):
                continue

            name = ""
            # Try to find name in lnkbtnName_ first
            name_link = name_cell.find("a", id=re.compile(r"lnkbtnName_"))
            if isinstance(name_link, Tag) and name_link.get_text(strip=True):
                name = name_link.get_text(strip=True)
            else:
                # If not found or empty, try lnkbtnLastName_
                name_link = name_cell.find("a", id=re.compile(r"lnkbtnLastName_"))
                if isinstance(name_link, Tag):
                    name = name_link.get_text(strip=True)

            # Third cell contains address components
            address_cell = cells[2]
            if not isinstance(address_cell, Tag):
                continue

            address_span = address_cell.find("span", id=re.compile(r"lblAddress_"))
            other_span = address_cell.find("span", id=re.compile(r"lblOther_"))
            city_span = address_cell.find("span", id=re.compile(r"lblCity_"))
            zip_span = address_cell.find("span", id=re.compile(r"lblZip_"))

            address = (
                address_span.get_text(strip=True)
                if isinstance(address_span, Tag)
                else ""
            )
            other = (
                other_span.get_text(strip=True) if isinstance(other_span, Tag) else ""
            )
            city = city_span.get_text(strip=True) if isinstance(city_span, Tag) else ""
            zip_code = (
                zip_span.get_text(strip=True) if isinstance(zip_span, Tag) else ""
            )

            # Only add row if we have at least a name
            if name:
                results.append(
                    {
                        "date": date,
                        "committee_name": name,
                        "address": address,
                        "other": other,
                        "city": city,
                        "zip_code": zip_code,
                    }
                )

        return results

    logger = dg.get_dagster_logger("ks_get_and_save_party_politicial_committee_data")
    name = "committees"
    output_dir = KS_DATA_PATH_PREFIX + "/" + name
    session = requests.Session()

    initial_res = session.get(KS_PAC_URL, headers=HEADERS)
    html_content = initial_res.text

    session.post(
        KS_PAC_ENTRY_URL,
        data=ks_build_initial_payload(html_content, name),
        headers=HEADERS,
    )

    cookie_string = "; ".join(
        [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
    )

    response = session.get(KS_PAC_URL, headers=get_pac_headers(cookie_string))

    html_content = response.text
    offices = ks_extract_offices(html_content)

    for office in offices:
        logger.info(f"Start Crawling With office: {office}...")
        payload = build_pac_search_payload(html_content, office)

        response = session.post(
            KS_PAC_SEARCH_URL, data=payload, headers=get_pac_headers(cookie_string)
        )

        response = session.get(
            KS_PAC_SEARCH_RESULTS_URL, headers=get_pac_headers(cookie_string)
        )

        # Parse the committee data from the response
        results = parse_committee_table(response.text)
        logger.info(f"Extracted {len(results)} committee records for office {office}")

        page = 1
        if results:
            ks_write_results_to_csv(
                results, f"{office}_pagination_{page}", output_dir, name
            )
        while True:
            last_page, ellipsis = ks_get_pagination_state(response.text)
            if not ellipsis and page >= last_page:
                logger.info("Reached final page — done.")
                break

            logger.info(f"Process Page: {page} For Office: {office}...")

            page += 1
            payload = ks_build_pagination_payload(
                response.text, page, "grdviewOrgResults"
            )
            payload = {**payload, "__VIEWSTATEENCRYPTED": ""}

            time.sleep(0.5 + random.uniform(0.5, 2))
            response = session.post(
                KS_PAC_SEARCH_RESULTS_URL,
                data=payload,
                headers=get_pac_headers(cookie_string),
            )

            results = parse_committee_table(response.text)
            if results:
                ks_write_results_to_csv(
                    results, f"{office}_pagination_{page}", output_dir, name
                )
                logger.info("--------------------------------")
                logger.info(
                    f"Extracted {len(results)} committee\
                            records for office {office} page {page}"
                )


@dg.asset(pool="ks_api")
def ks_get_and_save_candidate_data():
    """
    Scrapes Kansas candidate filing data by election and saves it to CSV files.

    This asset performs the following steps:
    1. Loads the initial candidate filing form from the Kansas Ethics website.
    2. Extracts all available election cycles from the dropdown.
    3. For each election, submits the form to retrieve candidate filings.
    4. Parses the candidate table and extracts relevant fields.
    5. Writes results to CSV, one file per election, organized in a
    structured output directory.

    Extracted fields include:
        - candidate
        - office
        - party
        - title
        - first_name
        - middle
        - last_name
        - suffix
        - address
        - city
        - state
        - zip
        - mailing_address
        - mailing_city
        - mailing_state
        - mailing_zip
        - home_phone
        - work_phone
        - cell_phone
        - email
        - web_address
        - date_filed
        - ballot_city

    Output:
        CSV files saved under: {KS_DATA_PATH_PREFIX}/candidate

    Notes:
        - Maintains ASP.NET form state using __VIEWSTATE, __EVENTVALIDATION, etc.
        - Submits the form programmatically for each election to fetch data.
        - Cleans up non-breaking space characters from table cells.
    """

    def parse_candidate_table(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr.gvResults")
        data = []
        for row in rows:
            cells = [
                cell.get_text(strip=True).replace("\xa0", "")
                for cell in row.find_all("td")
            ]

            data.append(
                {
                    "candidate": cells[0],
                    "office": cells[1],
                    "party": cells[2],
                    "title": cells[3],
                    "first_name": cells[4],
                    "middle": cells[5],
                    "last_name": cells[6],
                    "suffix": cells[7],
                    "address": cells[8],
                    "city": cells[9],
                    "state": cells[10],
                    "zip": cells[11],
                    "mailing_address": cells[12],
                    "mailing_city": cells[13],
                    "mailing_state": cells[14],
                    "mailing_zip": cells[15],
                    "home_phone": cells[16],
                    "work_phone": cells[17],
                    "cell_phone": cells[18],
                    "email": cells[19],
                    "web_address": cells[20],
                    "date_filed": cells[21],
                    "ballot_city": cells[22],
                }
            )
        return data

    logger = dg.get_dagster_logger("ks_get_and_save_candidate_data")
    session = requests.Session()
    resp = requests.get(KS_CANDIDATE_URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()

    html = resp.text

    elections = ks_get_election_values(html)

    for election in elections:
        payload = {
            "__VIEWSTATE": (ks_extract_hidden_input_value(html, "__VIEWSTATE")),
            "__VIEWSTATEGENERATOR": (
                ks_extract_hidden_input_value(html, "__VIEWSTATEGENERATOR")
            ),
            "__EVENTVALIDATION": (
                ks_extract_hidden_input_value(html, "__EVENTVALIDATION")
            ),
            "ddlElections": election,
            "btnSubmit": "Submit",
        }

        time.sleep(0.5 + random.uniform(0.5, 2))
        post_resp = session.post(KS_CANDIDATE_URL, data=payload, headers=HEADERS)

        post_resp.raise_for_status()

        candidates = parse_candidate_table(post_resp.text)
        logger.info(f"Election {election}: Found {len(candidates)} candidates")
        name = "candidate"
        output_dir = KS_DATA_PATH_PREFIX + "/" + name

        ks_write_results_to_csv(candidates, election, output_dir, name)


# -------------------------------
# Assets to insert data to landing table
# -------------------------------
@dg.asset(deps=["ks_get_and_save_contribution_data"])
def ks_insert_contribution_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Kansas receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_contribution.csv` files inside the
        `contribution/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ks_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ks_contribution_landing",
        table_columns_name=[
            "candidate_name",
            "contributor_name",
            "contributor_address",
            "contributor_city",
            "contributor_state",
            "contributor_zip",
            "occupation",
            "industry",
            "date_received",
            "tender_type",
            "amount",
            "in_kind_amount",
            "in_kind_description",
            "period_start",
            "period_end",
        ],
        data_validation_callback=lambda row: len(row) == 15,
        category="contribution",
    )


@dg.asset(deps=["ks_get_and_save_expenditure_data"])
def ks_insert_expenditure_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Kansas receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_expenditure.csv` files inside the
        `expenditure/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ks_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ks_expenditure_landing",
        table_columns_name=[
            "candidate_name",
            "recipient",
            "address",
            "city",
            "state",
            "zip_code",
            "date",
            "expenditure_description",
            "amount",
            "start_date",
            "end_date",
        ],
        data_validation_callback=lambda row: len(row) == 11,
        category="expenditure",
    )


@dg.asset(deps=["ks_get_and_save_party_politicial_committee_data"])
def ks_insert_committees_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Kansas receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_committees.csv` files inside the
        `committees/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ks_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ks_committees_landing",
        table_columns_name=[
            "date",
            "committee_name",
            "address",
            "other",
            "city",
            "zip_code",
        ],
        data_validation_callback=lambda row: len(row) == 6,
        category="committees",
    )


@dg.asset(deps=["ks_get_and_save_candidate_data"])
def ks_insert_candidate_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Loads Kansas receipt transactions CSVs into the PostgreSQL landing table.

    - Scans downloaded `*_candidate.csv` files inside the
        `candidate/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ks_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ks_candidate_landing",
        table_columns_name=[
            "candidate",
            "office",
            "party",
            "title",
            "first_name",
            "middle",
            "last_name",
            "suffix",
            "address",
            "city",
            "state",
            "zip",
            "mailing_address",
            "mailing_city",
            "mailing_state",
            "mailing_zip",
            "home_phone",
            "work_phone",
            "cell_phone",
            "email",
            "web_address",
            "date_filed",
            "ballot_city",
        ],
        data_validation_callback=lambda row: len(row) == 23,
        category="candidate",
    )

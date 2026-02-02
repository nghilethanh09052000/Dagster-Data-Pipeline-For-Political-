"""
These assets crawl the aspx page for Louisiana campaign finance data which requires
simulating human actions on the website:

1. Initial GET request to load the search form
2. POST request to submit search parameters
3. GET request to fetch search results

The flow follows a similar pattern to Alaska's implementation but is tailored
for Louisiana's specific form structure and requirements.
"""

import csv
import glob
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from bs4 import BeautifulSoup, Tag
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants for Louisiana
LA_DATA_PATH_PREFIX = "./states/louisiana"

# Base URLs
LA_BASE_URL = "https://www.ethics.la.gov"
LA_CONTRIBUTION_SEARCH_URL = (
    f"{LA_BASE_URL}/CampaignFinanceSearch/SearchEFilingContributors.aspx"
)
LA_CONTRIBUTION_RESULTS_URL = (
    f"{LA_BASE_URL}/CampaignFinanceSearch/SearchResultsByContributions.aspx"
)
LA_EXPENDITURE_SEARCH_URL = (
    f"{LA_BASE_URL}/CampaignFinanceSearch/SearchEfilingExpenditures.aspx"
)
LA_EXPENDITURE_RESULTS_URL = (
    f"{LA_BASE_URL}/CampaignFinanceSearch/SearchResultsByExpenditures.aspx"
)
LA_CANDIDATE_SEARCH_URL = f"{LA_BASE_URL}/CampaignFinanceSearch/SearchByNameAdv.aspx"
LA_CANDIDATE_RESULTS_URL = f"{LA_BASE_URL}/CampaignFinanceSearch/SearchResultsAdv.aspx"
LA_LOAD_SEARCH_URL = f"{LA_BASE_URL}/CampaignFinanceSearch/LoadSearch.aspx"
LA_PLEASE_WAIT_URL = f"{LA_BASE_URL}/CampaignFinanceSearch/PleaseWait.aspx"
LA_REDIRECT_URL = f"{LA_BASE_URL}/CampaignFinanceSearch/Redirect.aspx"

# INIITIAL_QUERY_HEADER for requests
HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "origin": LA_BASE_URL,
    "referer": LA_CONTRIBUTION_SEARCH_URL,
    "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A-Brand";v="8", "Chromium";v="135"',
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

"""
Utils Python
"""


def la_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
) -> dg.MaterializeResult:
    """Insert raw CSV data from previously downloaded files into the specified
    PostgreSQL landing table.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL.
        table_name (str): Name of the landing table to insert data into.
        table_columns_name (list[str]): List of column names in the correct order.
        data_validation_callback (Callable[[list[str]], bool]): Row validation function.
        category (str): The subdirectory under each year (e.g., 'contributions').

    Returns:
        dg.MaterializeResult: A Dagster metadata result object containing table name
        and row count.
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    base_path = Path(LA_DATA_PATH_PREFIX)
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


def la_extract_hidden_input_value(html_content: str, key: str) -> str:
    """Extract hidden input value from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    element = soup.find("input", {"type": "hidden", "name": key})
    if isinstance(element, Tag) and element.has_attr("value"):
        value = element["value"]
        return str(value) if value is not None else ""
    return ""


def la_get_contribution_search_payload(
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    committee_name: str = "",
    committee_id: str = "",
) -> dict[str, Any]:
    """Generate payload for contribution search POST request."""
    return {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$PerformSearchLinkButton",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__EVENTVALIDATION": event_validation,
        "ctl00$ContentPlaceHolder1$RadComboBox1": f"{committee_name}",
        "ctl00_ContentPlaceHolder1_RadComboBox1_ClientState": json.dumps(
            {
                "logEntries": [],
                "value": f"{committee_id}",
                "text": f"{committee_name}",
                "enabled": True,
                "checkedIndices": [],
                "checkedItemsTextOverflows": False,
            }
        ),
        "ctl00$ContentPlaceHolder1$NameHiddenField": f"{committee_id} ",
        "ctl00$ContentPlaceHolder1$DateFromRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateFromRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$DateToRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateToRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$ContributionsFromRadNumericTextBox": "",
        "ctl00_ContentPlaceHolder1_ContributionsFromRadNumericTextBox_ClientState": (
            json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "minValue": 0,
                    "maxValue": 70368744177664,
                    "lastSetTextBoxValue": "",
                }
            )
        ),
        "ctl00$ContentPlaceHolder1$ContributionsToRadNumericTextBox": "",
        "ctl00_ContentPlaceHolder1_ContributionsToRadNumericTextBox_ClientState": (
            json.dumps(
                {
                    "enabled": True,
                    "emptyMessage": "",
                    "validationText": "",
                    "valueAsString": "",
                    "minValue": 0,
                    "maxValue": 70368744177664,
                    "lastSetTextBoxValue": "",
                }
            )
        ),
        "ctl00$ContentPlaceHolder1$CityTextBox": "",
        "ctl00$ContentPlaceHolder1$StateTextBox": "",
        "ctl00$ContentPlaceHolder1$ZipTextBox": "",
        "ctl00$ContentPlaceHolder1$ContNameTextBox": "",
        "ctl00$ContentPlaceHolder1$DescTextBox": "",
        # "ctl00$ContentPlaceHolder1$OrderByDropDownList": (
        #     "filer_name ASC , filer_fname ASC"
        # ),
        # "ctl00$ContentPlaceHolder1$OrderThenByDropDownList": "expn_amt DESC",
    }


def la_extract_form_data(html_content: str) -> dict[str, str]:
    """Extract form data from HTML content."""
    return {
        "__VIEWSTATE": la_extract_hidden_input_value(html_content, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": la_extract_hidden_input_value(
            html_content, "__VIEWSTATEGENERATOR"
        ),
        "__EVENTVALIDATION": la_extract_hidden_input_value(
            html_content, "__EVENTVALIDATION"
        ),
    }


def la_get_expenditure_search_payload(
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    committee_name: str = "",
    committee_id: str = "",
) -> dict[str, Any]:
    """Generate payload for expenditure search POST request."""
    return {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$PerformSearchLinkButton",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__EVENTVALIDATION": event_validation,
        "ctl00$ContentPlaceHolder1$RadComboBox1": f"{committee_name}",
        "ctl00_ContentPlaceHolder1_RadComboBox1_ClientState": json.dumps(
            {
                "logEntries": [],
                "value": f"{committee_id}",
                "text": f"{committee_name}",
                "enabled": True,
                "checkedIndices": [],
                "checkedItemsTextOverflows": False,
            }
        ),
        "ctl00$ContentPlaceHolder1$NameHiddenField": f"{committee_id} ",
        "ctl00$ContentPlaceHolder1$DateFromRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateFromRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$DateToRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateToRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$ContributionsFromTextBox": "",
        "ctl00$ContentPlaceHolder1$ContributionsToTextBox": "",
        "ctl00$ContentPlaceHolder1$CityTextBox": "",
        "ctl00$ContentPlaceHolder1$StateTextBox": "",
        "ctl00$ContentPlaceHolder1$ZipTextBox": "",
        "ctl00$ContentPlaceHolder1$ContNameTextBox": "",
        "ctl00$ContentPlaceHolder1$DescTextBox": "",
        "ctl00$ContentPlaceHolder1$OrderByDropDownList": (
            "filer_name ASC , filer_fname ASC"
        ),
        "ctl00$ContentPlaceHolder1$OrderThenByDropDownList": "expn_amt DESC",
    }


def la_get_candidate_search_payload(
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    year: str = "",
) -> dict[str, Any]:
    """Generate payload for candidate search POST request."""
    return {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$SearchLinkButton",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__EVENTVALIDATION": event_validation,
        "ctl00$ContentPlaceHolder1$NameTextBox": "",
        "ctl00$ContentPlaceHolder1$OfficeTextBox": "",
        "ctl00$ContentPlaceHolder1$YearTextBox": year,
        "ctl00$ContentPlaceHolder1$DateFromRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateFromRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$DateToRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateToRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
    }


def la_get_headers(session: requests.Session, referer: str) -> dict[str, str]:
    """Get headers with session cookies."""
    return {
        "accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;"
            "q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "origin": LA_BASE_URL,
        "referer": referer,
        "sec-ch-ua": (
            '"Microsoft Edge";v="135", "Not-A-Brand";v="8", "Chromium";v="135"'
        ),
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
        "cookie": "; ".join([f"{k}={v}" for k, v in session.cookies.items()]),
    }


"""
Contribution Assets
"""


@dg.op(out=dg.DynamicOut(dict[str, str]))
def la_fetch_comittees_list_search_initial_page():
    """Fetch the initial contribution search page to get form data."""
    logger = dg.get_dagster_logger()

    session = requests.Session()
    response = session.get(LA_CONTRIBUTION_SEARCH_URL, headers=HEADERS)

    # Extract committee names and IDs from select element
    soup = BeautifulSoup(response.text, "html.parser")
    select = soup.find("select", {"id": "ComboBox_RadComboBox1"})
    committees = []

    if isinstance(select, Tag):
        options = select.find_all("option")
        for option in options:
            if isinstance(option, Tag) and option.has_attr("value"):
                committees.append(
                    {
                        "committee_name": option.text.strip(),
                        "committee_id": option["value"],
                    }
                )

    logger.info(f"Found {len(committees)} committees")

    # TODO: remove this limiter
    for committee in committees:
        yield dg.DynamicOutput(
            committee, mapping_key=str(committee["committee_id"]).strip()
        )


@dg.op()
def la_fetch_committee_contribution_data(
    committee: dict[str, str],
):
    """Fetch contribution data by submitting search form and downloading results."""
    logger = dg.get_dagster_logger()

    session = requests.Session()
    response = session.get(LA_CONTRIBUTION_SEARCH_URL, headers=HEADERS)

    # Extract form data
    viewstate = la_extract_hidden_input_value(response.text, "__VIEWSTATE")
    viewstate_generator = la_extract_hidden_input_value(
        response.text, "__VIEWSTATEGENERATOR"
    )
    event_validation = la_extract_hidden_input_value(response.text, "__EVENTVALIDATION")

    # Create output directory
    output_dir = Path(LA_DATA_PATH_PREFIX) / "contributions"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Processing committee: {committee['committee_name']}")
    committee_name = committee["committee_name"]
    committee_id = committee["committee_id"]

    # Submit search form for the committee
    payload = la_get_contribution_search_payload(
        viewstate=viewstate,
        viewstate_generator=viewstate_generator,
        event_validation=event_validation,
        committee_name=committee_name,
        committee_id=committee_id,
    )

    # Step 1: Initial POST request
    session.post(
        LA_CONTRIBUTION_SEARCH_URL,
        data=payload,
        headers=la_get_headers(session, LA_CONTRIBUTION_SEARCH_URL),
    )

    # Step 2: GET request to LoadSearch.aspx
    load_search_url = (
        f"{LA_LOAD_SEARCH_URL}?SearchPage=SearchResultsByContributions.aspx"
    )
    session.get(
        load_search_url,
        headers=la_get_headers(session, LA_CONTRIBUTION_SEARCH_URL),
    )

    # Step 3: GET request to PleaseWait.aspx
    session.get(
        LA_PLEASE_WAIT_URL,
        headers=la_get_headers(session, load_search_url),
    )

    # Step 4: GET request to Redirect.aspx
    redirect_url = f"{LA_REDIRECT_URL}?SearchPage=SearchResultsByContributions.aspx"
    session.get(
        redirect_url,
        headers=la_get_headers(session, LA_PLEASE_WAIT_URL),
    )

    # Step 5: GET request to SearchResultsByContributions.aspx
    results_response = session.get(
        LA_CONTRIBUTION_RESULTS_URL,
        headers=la_get_headers(session, redirect_url),
    )

    # Step 6: Extract form data from results page
    form_data = la_extract_form_data(results_response.text)

    # Step 7: POST request to export CSV
    export_payload = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": form_data["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": form_data["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": form_data["__EVENTVALIDATION"],
        "ctl00$ContentPlaceHolder1$ExportToCSVLinkButton": (
            "File is being generated.  Please check your downloads."
        ),
    }

    export_response = session.post(
        LA_CONTRIBUTION_RESULTS_URL,
        data=export_payload,
        headers=la_get_headers(session, LA_CONTRIBUTION_RESULTS_URL),
    )

    if (
        export_response.headers.get("content-type") is None
        or "text/csv" not in export_response.headers["content-type"]
    ):
        logger.error(
            "Contributions doesn't return csv! "
            "Committee Name: {committee['committee_name']}"
        )
        return ""

    # Save results with committee name in filename
    output_path = output_dir / f"{committee_id}_contributions.csv"
    with open(output_path, "wb") as f:
        f.write(export_response.content)

    logger.info(
        f"Successfully downloaded contribution data for "
        f"{committee['committee_name']} to {output_path}"
    )

    # return {"session": session, "output_dir": str(output_dir)}
    return str(output_dir)


@dg.op()
def combine_all_dir(committee: list[str]) -> int:
    """
    Honestly it's just here because on graph_asset we need to return something
    that's not from fan-out or fan-in
    """
    return len(committee)


@dg.graph_asset()
def la_fetch_contributions_data() -> None:
    """
    Tied all the ops together here.
    """

    committees = la_fetch_comittees_list_search_initial_page()
    committees_result = committees.map(la_fetch_committee_contribution_data)

    return combine_all_dir(committees_result.collect())


@dg.asset(deps=[la_fetch_contributions_data])
def la_inserting_contributions_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert contribution data into the landing table."""
    return la_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="la_contributions_landing",
        table_columns_name=[
            "filer_last_name",
            "filer_first_name",
            "report_code",
            "report_type",
            "report_number",
            "contributor_type_code",
            "contributor_name",
            "contributor_addr1",
            "contributor_addr2",
            "contributor_city",
            "contributor_state",
            "contributor_zip",
            "contribution_type",
            "contribution_description",
            "contribution_date",
            "contribution_amt",
            "contribution_designated_election_addition_info",
        ],
        data_validation_callback=lambda row: len(row) == 17,
        category="contributions",
    )


"""
Expenditures Assets
"""


@dg.asset
def la_fetch_expenditure_search_initial_page() -> dict:
    """Fetch the initial expenditure search page to get form data."""
    logger = dg.get_dagster_logger()

    session = requests.Session()
    response = session.get(LA_EXPENDITURE_SEARCH_URL, headers=HEADERS)

    # Extract form data
    viewstate = la_extract_hidden_input_value(response.text, "__VIEWSTATE")
    viewstate_generator = la_extract_hidden_input_value(
        response.text, "__VIEWSTATEGENERATOR"
    )
    event_validation = la_extract_hidden_input_value(response.text, "__EVENTVALIDATION")

    # Extract committee names and IDs from select element
    soup = BeautifulSoup(response.text, "html.parser")
    select = soup.find("select", {"id": "ComboBox_RadComboBox1"})
    committees = []
    if isinstance(select, Tag):
        options = select.find_all("option")
        for option in options:
            if isinstance(option, Tag) and option.has_attr("value"):
                committees.append(
                    {
                        "committee_name": option.text.strip(),
                        "committee_id": option["value"],
                    }
                )

    logger.info(f"Found {len(committees)} committees")

    return {
        "session": session,
        "viewstate": viewstate,
        "viewstate_generator": viewstate_generator,
        "event_validation": event_validation,
        "committees": committees,
    }


@dg.asset(deps=[la_fetch_expenditure_search_initial_page])
def la_fetch_expenditures_data(la_fetch_expenditure_search_initial_page: dict):
    """Fetch expenditure data by submitting search form and downloading results."""
    logger = dg.get_dagster_logger()

    session = la_fetch_expenditure_search_initial_page["session"]
    viewstate = la_fetch_expenditure_search_initial_page["viewstate"]
    viewstate_generator = la_fetch_expenditure_search_initial_page[
        "viewstate_generator"
    ]
    event_validation = la_fetch_expenditure_search_initial_page["event_validation"]
    committees = la_fetch_expenditure_search_initial_page["committees"]

    # Create output directory
    output_dir = Path(LA_DATA_PATH_PREFIX) / "expenditures"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Processing {len(committees)} committees...")

    for committee in committees:
        logger.info(f"Processing committee: {committee['committee_name']}")

        committee_name = committee["committee_name"]
        committee_id = committee["committee_id"]

        # Submit search form for the committee
        payload = la_get_expenditure_search_payload(
            viewstate=viewstate,
            viewstate_generator=viewstate_generator,
            event_validation=event_validation,
            committee_name=committee_name,
            committee_id=committee_id,
        )

        # Step 1: Initial POST request
        session.post(
            LA_EXPENDITURE_SEARCH_URL,
            data=payload,
            headers=la_get_headers(session, LA_EXPENDITURE_SEARCH_URL),
        )

        # Step 2: GET request to LoadSearch.aspx
        load_search_url = (
            f"{LA_LOAD_SEARCH_URL}?SearchPage=SearchResultsByExpenditures.aspx"
        )
        session.get(
            load_search_url,
            headers=la_get_headers(session, LA_EXPENDITURE_SEARCH_URL),
        )

        # Step 3: GET request to PleaseWait.aspx
        session.get(
            LA_PLEASE_WAIT_URL,
            headers=la_get_headers(session, load_search_url),
        )

        # Step 4: GET request to Redirect.aspx
        redirect_url = f"{LA_REDIRECT_URL}?SearchPage=SearchResultsByExpenditures.aspx"
        session.get(
            redirect_url,
            headers=la_get_headers(session, LA_PLEASE_WAIT_URL),
        )

        # Step 5: GET request to SearchResultsByExpenditures.aspx
        results_response = session.get(
            LA_EXPENDITURE_RESULTS_URL,
            headers=la_get_headers(session, redirect_url),
        )

        # Step 6: Extract form data from results page
        form_data = la_extract_form_data(results_response.text)

        # Step 7: POST request to export CSV
        export_payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": form_data["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": form_data["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": form_data["__EVENTVALIDATION"],
            "ctl00$ContentPlaceHolder1$ExportToCSVLinkButton": (
                "File is being generated.  Please check your downloads."
            ),
        }

        export_response = session.post(
            LA_EXPENDITURE_RESULTS_URL,
            data=export_payload,
            headers=la_get_headers(session, LA_EXPENDITURE_RESULTS_URL),
        )

        # Save results with committee name in filename
        output_path = output_dir / f"{committee_id}_expenditures.csv"
        with open(output_path, "wb") as f:
            f.write(export_response.content)

        logger.info(
            f"Successfully downloaded expenditure data for\
            {committee['committee_name']} to {output_path}"
        )

    return {"session": session, "output_dir": str(output_dir)}


@dg.asset(deps=[la_fetch_expenditures_data])
def la_inserting_expenditures_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert expenditure data into the landing table."""
    return la_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="la_expenditures_landing",
        table_columns_name=[
            "filer_last_name",
            "filer_first_name",
            "report_code",
            "report_type",
            "report_number",
            "schedule",
            "recipient_name",
            "recipient_addr1",
            "recipient_addr2",
            "recipient_city",
            "recipient_state",
            "recipient_zip",
            "expenditure_description",
            "candidate_beneficiary",
            "expenditure_date",
            "expenditure_amt",
        ],
        data_validation_callback=lambda row: len(row) == 16,
        category="expenditures",
    )


"""
Candidates Assets
"""


@dg.asset
def la_fetch_candidate_search_initial_page() -> dict:
    """Fetch the initial candidate search page to get form data."""
    logger = dg.get_dagster_logger()

    session = requests.Session()
    response = session.get(LA_CANDIDATE_SEARCH_URL, headers=HEADERS)

    # Extract form data
    viewstate = la_extract_hidden_input_value(response.text, "__VIEWSTATE")
    viewstate_generator = la_extract_hidden_input_value(
        response.text, "__VIEWSTATEGENERATOR"
    )
    event_validation = la_extract_hidden_input_value(response.text, "__EVENTVALIDATION")

    logger.info("Successfully fetched candidate search page")

    return {
        "session": session,
        "viewstate": viewstate,
        "viewstate_generator": viewstate_generator,
        "event_validation": event_validation,
    }


def la_parse_candidate_data(html_content: str) -> list[dict[str, str]]:
    """Parse candidate data from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_ResultsGridView"})
    if not isinstance(table, Tag):
        return []

    candidates = []
    for row in table.find_all("tr")[1:]:  # Skip header row
        if not isinstance(row, Tag):
            continue
        cells = row.find_all("td")
        if len(cells) >= 4:
            # Clean the data
            candidate_name = cells[0].text.strip()
            filer_type = cells[1].text.strip()
            election_date = cells[2].text.strip()
            office = cells[3].text.strip()

            # Skip rows that are not actual candidate data
            if "Page:" in candidate_name or not candidate_name:
                continue

            candidate = {
                "candidate_name": candidate_name,
                "filer_type": filer_type,
                "election_date": election_date,
                "office": office,
            }
            candidates.append(candidate)
    return candidates


def la_is_next_button_disabled(html_content: str) -> bool:
    """Check if the Next button is disabled."""
    soup = BeautifulSoup(html_content, "html.parser")
    next_button = soup.find(
        "input",
        {"id": "ctl00_ContentPlaceHolder1_ResultsGridView_ctl01_NextLinkButton"},
    )
    if isinstance(next_button, Tag):
        return "disabled" in next_button.attrs
    return False


@dg.asset(deps=[la_fetch_candidate_search_initial_page])
def la_fetch_candidates_data(la_fetch_candidate_search_initial_page: dict[str, Any]):
    """Fetch candidate data by submitting search form and downloading results."""
    logger = dg.get_dagster_logger()

    session = la_fetch_candidate_search_initial_page["session"]
    viewstate = la_fetch_candidate_search_initial_page["viewstate"]
    viewstate_generator = la_fetch_candidate_search_initial_page["viewstate_generator"]
    event_validation = la_fetch_candidate_search_initial_page["event_validation"]

    # Create output directory
    output_dir = Path(LA_DATA_PATH_PREFIX) / "candidates"
    output_dir.mkdir(parents=True, exist_ok=True)

    current_page = 1

    # Submit initial search payload
    payload = {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$SearchLinkButton",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__EVENTVALIDATION": event_validation,
        "ctl00$ContentPlaceHolder1$NameTextBox": "",
        "ctl00$ContentPlaceHolder1$OfficeTextBox": "",
        "ctl00$ContentPlaceHolder1$YearTextBox": "",
        "ctl00$ContentPlaceHolder1$DateFromRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateFromRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
        "ctl00$ContentPlaceHolder1$DateToRadDateInput": "",
        "ctl00_ContentPlaceHolder1_DateToRadDateInput_ClientState": json.dumps(
            {
                "enabled": True,
                "emptyMessage": "",
                "validationText": "",
                "valueAsString": "",
                "minDateStr": "1800-01-01-00-00-00",
                "maxDateStr": "2099-12-31-00-00-00",
                "lastSetTextBoxValue": "",
            }
        ),
    }

    # Submit initial search request
    response = session.post(
        LA_CANDIDATE_SEARCH_URL,
        data=payload,
        headers=la_get_headers(session, LA_CANDIDATE_SEARCH_URL),
    )

    # Extract form data from results page
    form_data = la_extract_form_data(response.text)

    while True:
        logger.info(f"Fetching candidates page {current_page}")

        # Submit pagination payload
        pagination_payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": form_data["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": form_data["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": form_data["__EVENTVALIDATION"],
            "ctl00$ContentPlaceHolder1$ResultsGridView$ctl01$GotoPageTextBox": str(
                current_page
            ),
            "ctl00$ContentPlaceHolder1$ResultsGridView$ctl01$NextLinkButton": "Next",
            "ctl00$ContentPlaceHolder1$ResultsGridView$ctl24$GotoPageTextBox": str(
                current_page
            ),
        }

        # Submit pagination request
        response = session.post(
            LA_CANDIDATE_RESULTS_URL,
            data=pagination_payload,
            headers=la_get_headers(session, LA_CANDIDATE_RESULTS_URL),
        )

        # Extract candidates from current page
        candidates = la_parse_candidate_data(response.text)
        logger.info(f"Found {len(candidates)} candidates on page {current_page}")

        # Save results for current page
        output_path = output_dir / f"{current_page}_candidates.csv"
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["candidate_name", "filer_type", "election_date", "office"],
            )
            writer.writeheader()
            writer.writerows(candidates)

        logger.info(f"Saved {len(candidates)} candidates to {output_path}")

        # Check if next button is disabled
        if la_is_next_button_disabled(response.text):
            break

        # Update form data for next page
        form_data = la_extract_form_data(response.text)
        current_page += 1

    return {"session": session, "output_dir": str(output_dir)}


@dg.asset(deps=[la_fetch_candidates_data])
def la_inserting_candidates_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert candidate data into the landing table."""
    return la_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="la_candidates_landing",
        table_columns_name=["candidate_name", "filer_type", "election_date", "office"],
        data_validation_callback=lambda row: len(row) == 4,
        category="candidates",
    )

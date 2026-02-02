"""
These assets crawl the aspx page which have to simulate
human action on website that:

- Come to the first page
- Press Search Button
- Press Expoprt Button
- Call CSV Data

I Follow this method:
https://www.youtube.com/watch?v=WZshV5nYVQc
"""

import csv
import glob
import json
import os
import re
import sys
from collections.abc import Callable
from datetime import datetime
from logging import Logger
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

csv.field_size_limit(sys.maxsize)

AK_DATA_PATH_PREFIX = "./states/alaska"
AK_FIRST_YEAR_DATA_AVAILABLE = datetime(year=2014, month=1, day=1)

# Partition definition for yearly processing
ak_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=AK_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)

AK_ALL_CANDIDATES_URL = (
    "https://aws.state.ak.us/apocreports/campaign/AllCandidates.aspx?type=all"
)
AK_CAMPAIGN_DISCLOSURE_FORM_URL = (
    "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDForms.aspx"
)
AK_CAMPAIGN_DISCLOSURE_INCOME_URL = (
    "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDIncome.aspx"
)
AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_URL = (
    "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDExpenditures.aspx"
)
AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_URL = (
    "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDTransactions.aspx"
)
AK_CAMPAIGN_DISCLOSURE_DEBT_URL = (
    "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDDebt.aspx"
)
AK_CONTRIBUTION_REPORTS_URL = (
    "https://aws.state.ak.us/apocreports/StatementContributions/SCForms.aspx"
)

AK_ALL_CANDIDATES_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/Campaign/AllCandidates.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CAMPAIGN_DISCLOSURE_FORM_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDForms.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CAMPAIGN_DISCLOSURE_INCOME_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDIncome.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDExpenditures.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDTransactions.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CAMPAIGN_DISCLOSURE_DEBT_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/CampaignDisclosure/CDDebt.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"
AK_CONTRIBUTION_REPORTS_DOWNLOAD_URL = "https://aws.state.ak.us/apocreports/StatementContributions/SCForms.aspx?exportAll=True&exportFormat=CSV&isExport=True&pageSize=20&pageIndex=0"

HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "connection": "keep-alive",
    "host": "aws.state.ak.us",
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

"""
- Common Extract For viewstate Functions
- Theses Viewstates are used for simulate human
behavior, so that the website would not know bot
and avoid user blocking

"""


def ak_extract_year_options(html_content, name):
    soup = BeautifulSoup(html_content, "html.parser")
    year_select = soup.find("select", {"name": name})

    if not year_select:
        return []

    year_options = []

    # Type cast year_select to avoid type errors
    if isinstance(year_select, Tag):
        for option in year_select.find_all("option"):
            # Additional type check for option
            if isinstance(option, Tag):
                value = option.get("value")
                if isinstance(value, str) and value.isdigit():
                    year_options.append(int(value))

    return year_options


def ak_extract_hidden_input_value(html_content, key):
    soup = BeautifulSoup(html_content, "html.parser")
    element = soup.find("input", {"type": "hidden", "name": key})

    from bs4.element import Tag

    if isinstance(element, Tag) and element.has_attr("value"):
        return element["value"]

    if key == "__VIEWSTATE":
        match = re.search(r"__VIEWSTATE\|(.*?)\|8\|hiddenField\|", html_content)
        return match.group(1) if match else ""
    elif key == "__VIEWSTATEGENERATOR":
        match = re.search(r"__VIEWSTATEGENERATOR\|([A-Za-z0-9]+)\|", html_content)
        return match.group(1) if match else ""
    return ""


def ak_extract_search_button(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    search_button = soup.find("input", {"type": "submit", "value": "Search"})

    from bs4.element import Tag

    if isinstance(search_button, Tag) and search_button.has_attr("name"):
        return search_button["name"]
    return ""


def ak_extract_export_button(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    search_button = soup.find("input", {"type": "submit", "value": "Export"})

    from bs4.element import Tag

    if isinstance(search_button, Tag) and search_button.has_attr("name"):
        return search_button["name"]
    return ""


####################################################


"""
- Extract Form Data For Each Assets Functions
- Each Asset Using Different formdata, so I am
about to create separate value for each

"""


def ak_get_candidate_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    if btn_search:
        data = {
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            "M$C$ddlYear": year,
            "M$C$txtName": "",
            "M$C$grid$ctl00$ctl03$ctl01$PageSizeComboBox": "50",
            "M_C_grid_ctl00_ctl03_ctl01_PageSizeComboBox_ClientState": "",
            "M_C_grid_rghcMenu_ClientState": "",
            "M_C_grid_ClientState": "",
            "M_C_ctl00_ClientState": json.dumps(
                {
                    "text": "View",
                    "value": "",
                    "checked": False,
                    "target": "",
                    "navigateUrl": "",
                    "commandName": "",
                    "commandArgument": "",
                    "autoPostBack": True,
                    "selectedToggleStateIndex": 0,
                    "validationGroup": None,
                    "readOnly": False,
                    "primary": False,
                    "enabled": True,
                }
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "__ASYNCPOST": "true",
            "M$C$btnSearch": "Search",
        }
    else:
        data = {
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            "M$C$ddlYear": "All",
            "M$C$txtName": "",
            "M$C$grid$ctl00$ctl03$ctl01$PageSizeComboBox": 20,
            "M_C_grid_ctl00_ctl03_ctl01_PageSizeComboBox_ClientState": "",
            "M_C_grid_rghcMenu_ClientState": "",
            "M_C_grid_ClientState": "",
            "M_C_ctl00_ClientState": json.dumps(
                {
                    "text": "View",
                    "value": "",
                    "checked": False,
                    "target": "",
                    "navigateUrl": "",
                    "commandName": "",
                    "commandArgument": "",
                    "autoPostBack": True,
                    "selectedToggleStateIndex": 0,
                    "validationGroup": None,
                    "readOnly": False,
                    "primary": False,
                    "enabled": True,
                }
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategenerator,
            "__ASYNCPOST": "true",
            export_btn_name: "Export",
        }

    return data


def ak_get_campaign_disclosure_form_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "M$C$csfFilter$ddlNameType": "Any",
        "M$C$csfFilter$ddlField": "CDFilerTypes",
        "M$C$csfFilter$ddlReportYear": str(year),
        "M$C$csfFilter$ddlStatus": "Default",
        "M$C$csfFilter$txtBeginDate": "",
        "M$C$csfFilter$txtEndDate": "",
        "M$C$csfFilter$txtName": "",
        "M$C$csfFilter$ddlValue": "-1",
        "M$C$grid$ctl00$ctl03$ctl01$PageSizeComboBox": "50",
        "M_C_grid_ctl00_ctl03_ctl01_PageSizeComboBox_ClientState": "",
        "M_C_grid_rghcMenu_ClientState": "",
        "M_C_grid_ClientState": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__ASYNCPOST": "true",
    }
    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


def ak_get_campaign_disclosure_income_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "M$C$sCDTransactions$csfFilter$ddlNameType": "CandidateName",
        "M$C$sCDTransactions$csfFilter$ddlField": "IncomeTypes",
        "M$C$sCDTransactions$csfFilter$ddlReportYear": str(year),
        "M$C$sCDTransactions$csfFilter$ddlStatus": "Default",
        "M$C$sCDTransactions$csfFilter$txtBeginDate": "",
        "M$C$sCDTransactions$csfFilter$txtEndDate": "",
        "M$C$sCDTransactions$csfFilter$txtName": "",
        "M$C$sCDTransactions$csfFilter$ddlValue": "-1",
        "M$C$sCDTransactions$grid$ctl00$ctl03$ctl01$PageSizeComboBox": "50",
        "M_C_sCDTransactions_grid_ctl00_ctl03_ctl01_PageSizeComboBox_ClientState": "",
        "M_C_sCDTransactions_grid_rghcMenu_ClientState": "",
        "M_C_sCDTransactions_grid_ClientState": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__ASYNCPOST": "true",
    }

    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


def ak_get_campaign_disclosure_expenditures_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "M$C$sCDTransactions$csfFilter$ddlNameType": "CandidateName",
        "M$C$sCDTransactions$csfFilter$ddlField": "IncomeTypes",
        "M$C$sCDTransactions$csfFilter$ddlReportYear": str(year),
        "M$C$sCDTransactions$csfFilter$ddlStatus": "Default",
        "M$C$sCDTransactions$csfFilter$txtBeginDate": "",
        "M$C$sCDTransactions$csfFilter$txtEndDate": "",
        "M$C$sCDTransactions$csfFilter$txtName": "",
        "M$C$sCDTransactions$csfFilter$ddlValue": "-1",
        "M$C$sCDTransactions$grid$ctl00$ctl03$ctl01$PageSizeComboBox": "50",
        "M_C_sCDTransactions_grid_ctl00_ctl03_ctl01_PageSizeComboBox_ClientState": "",
        "M_C_sCDTransactions_grid_rghcMenu_ClientState": "",
        "M_C_sCDTransactions_grid_ClientState": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__ASYNCPOST": "true",
    }
    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


def ak_get_campaign_disclosure_transations_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "M$C$sCDTransaction$csfFilter$ddlNameType": "CandidateName",
        "M$C$sCDTransaction$csfFilter$ddlField": "CDTransactionTypes",
        "M$C$sCDTransaction$csfFilter$ddlReportYear": str(year),
        "M$C$sCDTransaction$csfFilter$ddlStatus": "Default",
        "M$C$sCDTransaction$csfFilter$txtBeginDate": "",
        "M$C$sCDTransaction$csfFilter$txtEndDate": "",
        "M$C$sCDTransaction$csfFilter$txtName": "",
        "M$C$sCDTransaction$csfFilter$ddlValue": "-1",
        "__ASYNCPOST": "true",
    }
    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


def ak_get_campaign_disclosure_debt_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__VIEWSTATE": viewstate,
        "M$C$csfFilter$ddlNameType": "Any",
        "M$C$csfFilter$ddlField": "CDFilerTypes",
        "M$C$csfFilter$ddlReportYear": str(year),
        "M$C$csfFilter$ddlStatus": "Default",
        "M$C$csfFilter$txtBeginDate": "",
        "M$C$csfFilter$txtEndDate": "",
        "M$C$csfFilter$txtName": "",
        "M$C$csfFilter$ddlValue": "-1",
        "M_C_grid_rghcMenu_ClientState": "",
        "M_C_grid_ClientState": "",
        "__ASYNCPOST": "true",
    }
    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


def ak_get_contribution_reports_form_data(
    year: int,
    viewstate: str,
    viewstategenerator: str,
    btn_search: str | None = None,
    export_btn_name: str | None = None,
) -> dict:
    default_data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATEGENERATOR": viewstategenerator,
        "__VIEWSTATE": viewstate,
        "M$C$csfFilter$ddlNameType": "Any",
        "M$C$csfFilter$ddlField": "CDFilerTypes",
        "M$C$csfFilter$ddlReportYear": str(year),
        "M$C$csfFilter$ddlStatus": "Default",
        "M$C$csfFilter$txtBeginDate": "",
        "M$C$csfFilter$txtEndDate": "",
        "M$C$csfFilter$txtName": "",
        "M$C$csfFilter$ddlValue": "-1",
        "M_C_grid_rghcMenu_ClientState": "",
        "M_C_grid_ClientState": "",
        "__ASYNCPOST": "true",
    }
    if export_btn_name:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{export_btn_name}",
            export_btn_name: "Export",
        }
    else:
        data = {
            **default_data,
            "M$ctl19": f"M$UpdatePanel|{btn_search}",
            btn_search: "Search",
        }

    return data


####################################################
"""
Insert To Landing Table Function
"""


def ak_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
    year: int | None = None,
    truncate_query: sql.Composed | None = None,
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

        year (int, optional): Specific year to process. If None, processes all years.

        truncate_query (sql.Composed, optional): Custom SQL query to delete data
        by year.
                If provided, this query will be used instead of the default
                truncate.

    Returns:
        MaterializeResult: A Dagster metadata result object containing
                            the table name and the number of rows inserted.

    Process:
        - If year is specified, only processes files for that year.
        - Otherwise scans folders from 2014 to the current year for CSVs in the
          target category.
        - Uses custom truncate query if provided, otherwise truncates the
          destination table.
        - Validates and inserts data row-by-row using the provided validation function.
        - Logs all steps and errors.
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    base_path = Path(AK_DATA_PATH_PREFIX)
    data_files = []

    glob_path = base_path / category

    if glob_path.exists():
        if year is not None:
            # Process only files for the specific year
            files = glob.glob(str(glob_path / f"{year}_{category}.csv"))
        else:
            # Process all files in the category
            files = glob.glob(str(glob_path / f"*_{category}.csv"))
        data_files.extend(files)

    logger.info(f"Category: {category} | Found {len(data_files)} files.")

    # Use custom truncate query if provided, otherwise use default truncate
    if truncate_query is None:
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


####################################################


"""
Common Call Request Functions
"""


def ak_fetch_initial_page(
    session, logger: Logger, url: str, year_name: str | None = None
) -> dict:
    logger.info(f"Fetch Initial Page For Url: {url}...")

    response = session.get(url, headers=HEADERS)
    html_content = response.text

    viewstate = ak_extract_hidden_input_value(html_content, "__VIEWSTATE")
    viewstategenerator = ak_extract_hidden_input_value(
        html_content, "__VIEWSTATEGENERATOR"
    )
    btn_search = ak_extract_search_button(html_content)
    years = ak_extract_year_options(html_content, year_name)

    data = {
        "session": session,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "btn_search": btn_search,
        "years": years,
    }
    return data


def ak_fetch_search_button_page(session, url, data):
    response = session.post(url=url, data=data, headers=HEADERS, timeout=36000)

    html_content = response.text

    viewstate = ak_extract_hidden_input_value(html_content, "__VIEWSTATE")

    viewstategenerator = ak_extract_hidden_input_value(
        html_content, "__VIEWSTATEGENERATOR"
    )

    export_btn_name = ak_extract_export_button(html_content)

    return viewstate, viewstategenerator, export_btn_name


"""
Assets For Calling Initial Request
"""


@dg.asset()
def ak_fetch_all_candidates_initial_page() -> dict:
    """
    Fetches the initial HTML form data required to paginate through all Alaska
    candidate records on the campaign disclosure website.

    https://aws.state.ak.us/apocreports/Campaign/AllCandidates.aspx?type=all

    This asset is responsible for submitting the initial request to the
    candidate listing page and capturing the hidden form fields such as
    __VIEWSTATE and __EVENTVALIDATION, which are necessary to simulate
    subsequent postbacks for pagination.

    Returns:
        dict: A dictionary containing hidden form data (e.g., VIEWSTATE)
        and other necessary values required for the next pagination requests
    """
    logger = dg.get_dagster_logger("ak_fetch_all_candidates_initial_page")
    session = requests.Session()
    data = ak_fetch_initial_page(
        session, logger, url=AK_ALL_CANDIDATES_URL, year_name="M$C$ddlYear"
    )
    logger.info(
        "Form Data All Candidate \
                Fetched For Next Stage"
    )

    return data


@dg.asset()
def ak_fetch_campaign_disclosure_form_initial_page() -> dict:
    """
    Fetches the initial HTML form data required to paginate through Alaska's
    campaign disclosure forms on the official campaign finance site

    https://aws.state.ak.us/apocreports/CampaignDisclosure/CDForms.aspx

    This asset simulates the initial page load and extracts hidden fields such as
    __VIEWSTATE and __EVENTVALIDATION. These values are essential for constructing
    valid POST requests to navigate through paginated disclosure form records

    Returns:
        dict: A dictionary containing the hidden form data and any other necessary
        metadata needed for pagination and subsequent form submissions
    """

    logger = dg.get_dagster_logger("ak_fetch_campaign_disclosure_form_initial_page")
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_FORM_URL,
        year_name="M$C$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Campaign Disclosure Form \
                Fetched For Next Stage"
    )

    return data


@dg.asset()
def ak_fetch_campaign_disclosure_income_initial_page() -> dict:
    """
    Fetches the initial HTML form data needed to paginate through Alaska's
    campaign disclosure income records.

    https://aws.state.ak.us/apocreports/CampaignDisclosure/CDIncome.aspx

    Simulates a browser request to retrieve hidden fields such as __VIEWSTATE
    and __EVENTVALIDATION from the income disclosure form. These values are
    required to construct valid POST requests for paginated results

    Returns:
        dict: A dictionary containing form state data and other metadata
        necessary for navigating the campaign disclosure income pages.
    """
    logger = dg.get_dagster_logger("ak_fetch_campaign_disclosure_income_initial_page")
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_INCOME_URL,
        year_name="M$C$sCDTransactions$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Campaign Disclosure Income \
                Fetched For Next Stage.."
    )

    return data


@dg.asset()
def ak_fetch_campaign_disclosure_expenditures_initial_page() -> dict:
    """
    Fetches the initial HTML form data required to paginate through Alaska's
    campaign disclosure expenditure records.

    https://aws.state.ak.us/apocreports/CampaignDisclosure/CDExpenditures.aspx

    Extracts hidden fields needed for form-based navigation of the disclosure
    expenditures. These are used to simulate POST requests for scraping data
    across multiple pages.

    Returns:
        dict: A dictionary with hidden form fields and metadata to support
        subsequent expenditure data extraction.
    """
    logger = dg.get_dagster_logger(
        "ak_fetch_campaign_disclosure_expenditures_initial_page"
    )
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_URL,
        year_name="M$C$sCDTransactions$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Campaign Disclosure Expenditure \
                Fetched For Next Stage.."
    )

    return data


@dg.asset()
def ak_fetch_campaign_disclosure_transactions_initial_page() -> dict:
    """
    Fetches the initial form data for Alaska's campaign disclosure transactions page.

    Collects stateful form values such as __VIEWSTATE to enable pagination
    through the transaction dataset, which includes both income and expenditure
    activities.

    https://aws.state.ak.us/apocreports/CampaignDisclosure/CDTransactions.aspx

    Returns:
        dict: Form metadata required for constructing follow-up POST requests
        to fetch paginated campaign transaction data.
    """
    logger = dg.get_dagster_logger(
        "ak_fetch_campaign_disclosure_transactions_initial_page"
    )
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_URL,
        year_name="M$C$sCDTransaction$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Campaign Disclosure Transactions \
                Fetched For Next Stage.."
    )

    return data


@dg.asset()
def ak_fetch_campaign_disclosure_debt_initial_page() -> dict:
    """
    Fetches the initial form data required to scrape debt-related campaign
    disclosure records in Alaska.

    Gathers viewstate and other hidden fields used to manage session state
    across paginated requests on the debt disclosure web interface.

    https://aws.state.ak.us/apocreports/CampaignDisclosure/CDDebt.aspx

    Returns:
        dict: A dictionary of hidden form inputs and state values needed
        to request further pages of debt disclosure data
    """
    logger = dg.get_dagster_logger("ak_fetch_campaign_disclosure_debt_initial_page")
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_DEBT_URL,
        year_name="M$C$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Campaign Disclosure Debt \
                Fetched For Next Stage.."
    )

    return data


@dg.asset()
def ak_fetch_contribution_reports_initial_page() -> dict:
    """
    Fetches the initial form data required to scrape debt-related contribution
    reports records in Alaska.

    Gathers viewstate and other hidden fields used to manage session state
    across paginated requests on the debt disclosure web interface.

    https://aws.state.ak.us/apocreports/StatementContributions/SCForms.aspx

    Returns:
        dict: A dictionary of hidden form inputs and state values needed
        to request further pages of debt disclosure data
    """
    logger = dg.get_dagster_logger("ak_fetch_contribution_reports_initial_page")
    session = requests.Session()
    data = ak_fetch_initial_page(
        session,
        logger,
        url=AK_CAMPAIGN_DISCLOSURE_DEBT_URL,
        year_name="M$C$csfFilter$ddlReportYear",
    )
    logger.info(
        "Form Data Contribution Reports \
                Fetched For Next Stage.."
    )

    return data


####################################################

"""
Assets For Searching Table Data Request
"""


@dg.asset(
    deps=[ak_fetch_all_candidates_initial_page], partitions_def=ak_yearly_partition
)
def ak_search_all_candidates_table_data(
    context: dg.AssetExecutionContext, ak_fetch_all_candidates_initial_page: dict
):
    """
    Fetches the form data related to all candidates for a specific year
    from the Alaska campaign disclosure site, and prepares the data
    for the next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_all_candidates_initial_page (dict): Contains initial session details,
                viewstate, viewstategenerator, and search button data.

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year.
    """
    logger = dg.get_dagster_logger("ak_search_all_candidates_table_data")
    year = int(context.partition_key)

    session = ak_fetch_all_candidates_initial_page["session"]
    viewstate = ak_fetch_all_candidates_initial_page["viewstate"]
    viewstategenerator = ak_fetch_all_candidates_initial_page["viewstategenerator"]
    btn_search = ak_fetch_all_candidates_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_ALL_CANDIDATES_URL,
        data=ak_get_candidate_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Button All Candidates \
            Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_campaign_disclosure_form_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_campaign_disclosure_form_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_campaign_disclosure_form_initial_page: dict,
):
    """
    Fetches the form data related to campaign disclosure forms for a specific year
    from the Alaska campaign disclosure site, and prepares the data for
    the next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_campaign_disclosure_form_initial_page (dict):
                        Contains initial session details,
                        viewstate, viewstategenerator, and search button data

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year

    """
    logger = dg.get_dagster_logger("ak_search_campaign_disclosure_form_table_data")
    year = int(context.partition_key)

    session = ak_fetch_campaign_disclosure_form_initial_page["session"]
    viewstate = ak_fetch_campaign_disclosure_form_initial_page["viewstate"]
    viewstategenerator = ak_fetch_campaign_disclosure_form_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_campaign_disclosure_form_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CAMPAIGN_DISCLOSURE_FORM_URL,
        data=ak_get_campaign_disclosure_form_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Campaign Disclosure Form \
            Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_campaign_disclosure_income_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_campaign_disclosure_income_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_campaign_disclosure_income_initial_page: dict,
):
    """
    Fetches the form data related to campaign disclosure income for a specific year
    from the Alaska campaign disclosure site, and prepares the data for the
    next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_campaign_disclosure_income_initial_page (dict):
                            Contains initial session details,
                            viewstate, viewstategenerator, and search button data.

    Returns: dict: A dictionary containing session, year, viewstate,
    viewstategenerator, and export button data for the specific year.

    """
    logger = dg.get_dagster_logger("ak_search_campaign_disclosure_income_table_data")
    year = int(context.partition_key)

    session = ak_fetch_campaign_disclosure_income_initial_page["session"]
    viewstate = ak_fetch_campaign_disclosure_income_initial_page["viewstate"]
    viewstategenerator = ak_fetch_campaign_disclosure_income_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_campaign_disclosure_income_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CAMPAIGN_DISCLOSURE_INCOME_URL,
        data=ak_get_campaign_disclosure_income_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Campaign Disclosure Income \
        Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_campaign_disclosure_expenditures_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_campaign_disclosure_expenditures_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_campaign_disclosure_expenditures_initial_page: dict,
):
    """
    Fetches the form data related to campaign disclosure expenditures
    for a specific year
    from the Alaska campaign disclosure site, and prepares the data for the
    next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_campaign_disclosure_expenditures_initial_page (dict):
                        Contains initial session details,
                        viewstate, viewstategenerator, and search button data.

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year.

    """
    logger = dg.get_dagster_logger(
        "ak_search_campaign_disclosure_expenditures_table_data"
    )
    year = int(context.partition_key)

    session = ak_fetch_campaign_disclosure_expenditures_initial_page["session"]
    viewstate = ak_fetch_campaign_disclosure_expenditures_initial_page["viewstate"]
    viewstategenerator = ak_fetch_campaign_disclosure_expenditures_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_campaign_disclosure_expenditures_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_URL,
        data=ak_get_campaign_disclosure_expenditures_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Campaign Disclosure Expenditures \
            Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_campaign_disclosure_transactions_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_disclosure_transations_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_campaign_disclosure_transactions_initial_page: dict,
):
    """
    Fetches the form data related to campaign disclosure transactions for a specific
    year from the Alaska campaign disclosure site, and prepares the data for the
    next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_campaign_disclosure_transactions_initial_page (dict):
                    Contains initial session details,
                    viewstate, viewstategenerator, and search button data.

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year.

    """
    logger = dg.get_dagster_logger("ak_search_disclosure_transations_table_data")
    year = int(context.partition_key)

    session = ak_fetch_campaign_disclosure_transactions_initial_page["session"]
    viewstate = ak_fetch_campaign_disclosure_transactions_initial_page["viewstate"]
    viewstategenerator = ak_fetch_campaign_disclosure_transactions_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_campaign_disclosure_transactions_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_URL,
        data=ak_get_campaign_disclosure_transations_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Campaign Disclosure Transactions \
            Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_campaign_disclosure_debt_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_campaign_disclosure_debt_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_campaign_disclosure_debt_initial_page: dict,
):
    """
    Fetches the form data related to campaign disclosure debts for a specific year
    from the Alaska campaign disclosure site, and prepares the data for the
    next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_campaign_disclosure_debt_initial_page (dict):
                    Contains initial session details,
                    viewstate, viewstategenerator, and search button data.

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year.
    """
    logger = dg.get_dagster_logger("ak_search_campaign_disclosure_debt_table_data")
    year = int(context.partition_key)

    session = ak_fetch_campaign_disclosure_debt_initial_page["session"]
    viewstate = ak_fetch_campaign_disclosure_debt_initial_page["viewstate"]
    viewstategenerator = ak_fetch_campaign_disclosure_debt_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_campaign_disclosure_debt_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CAMPAIGN_DISCLOSURE_DEBT_URL,
        data=ak_get_campaign_disclosure_debt_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Campaign Disclosure Debts \
            Fetched For Next Stage for year {year}.."
    )

    return info


@dg.asset(
    deps=[ak_fetch_contribution_reports_initial_page],
    partitions_def=ak_yearly_partition,
)
def ak_search_contribution_reports_table_data(
    context: dg.AssetExecutionContext,
    ak_fetch_contribution_reports_initial_page: dict,
):
    """
    Fetches the form data related to contribution reports for a specific year
    from the Alaska contribution reports site, and prepares the data for the
    next stage in the pipeline.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_fetch_contribution_reports_initial_page (dict):
                    Contains initial session details,
                    viewstate, viewstategenerator, and search button data.

    Returns:
        dict: A dictionary containing session, year, viewstate,
              viewstategenerator, and export button data for the specific year.
    """
    logger = dg.get_dagster_logger("ak_search_contribution_reports_table_data")
    year = int(context.partition_key)

    session = ak_fetch_contribution_reports_initial_page["session"]
    viewstate = ak_fetch_contribution_reports_initial_page["viewstate"]
    viewstategenerator = ak_fetch_contribution_reports_initial_page[
        "viewstategenerator"
    ]
    btn_search = ak_fetch_contribution_reports_initial_page["btn_search"]

    # Cast viewstate and viewstategenerator to string to avoid type errors
    result = ak_fetch_search_button_page(
        session=session,
        url=AK_CONTRIBUTION_REPORTS_URL,
        data=ak_get_contribution_reports_form_data(
            year=year,
            viewstate=str(viewstate),
            viewstategenerator=str(viewstategenerator),
            btn_search=btn_search,
        ),
    )
    viewstate, viewstategenerator, export_btn_name = result

    info = {
        "session": session,
        "year": year,
        "viewstate": viewstate,
        "viewstategenerator": viewstategenerator,
        "export_btn_name": export_btn_name,
    }

    logger.info(
        f"Form Data Export Contribution Reports \
            Fetched For Next Stage for year {year}.."
    )

    return info


"""
Assets For Opening Form Export Data Request
"""


@dg.asset(
    deps=[ak_search_all_candidates_table_data], partitions_def=ak_yearly_partition
)
def ak_opening_all_candidates_form_export_data(
    context: dg.AssetExecutionContext, ak_search_all_candidates_table_data: dict
):
    """
    This function opens the all candidates form export data for a specific year.

    It retrieves form data for the candidate year and sends a POST request
    to the Alaska State campaign website to initiate the data export.
    Once the data is ready, the function downloads the CSV file and
    saves it to a specified directory, with the file named based on the year of
    the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
    `all_candidates`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_all_candidates_table_data (dict): A dictionary
        containing session, year,
        viewstate, viewstategenerator,
        and export button information fetched in previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger("ak_opening_all_candidates_form_export_data")
    year = int(context.partition_key)

    session = ak_search_all_candidates_table_data["session"]
    viewstate = ak_search_all_candidates_table_data["viewstate"]
    viewstategenerator = ak_search_all_candidates_table_data["viewstategenerator"]
    export_btn_name = ak_search_all_candidates_table_data["export_btn_name"]

    payload = ak_get_candidate_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_ALL_CANDIDATES_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_ALL_CANDIDATES_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "all_candidates")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_all_candidates.csv"

    data = session.get(url=AK_ALL_CANDIDATES_DOWNLOAD_URL, headers=HEADERS, stream=True)

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for \
                ak_opening_all_candidates_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_campaign_disclosure_form_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_campaign_disclosure_form_form_export_data(
    context: dg.AssetExecutionContext,
    ak_search_campaign_disclosure_form_table_data: dict,
):
    """
    This function opens the campaign disclosure form export data for a specific year.

    It retrieves form data for the year and sends a POST request to the
    Alaska State campaign
    website to initiate the data export. Once the data is ready, the function downloads
    the CSV file and saves it to a specified directory, with the file named based
    on the year of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
        `campaign_disclosure_form`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_campaign_disclosure_form_table_data (dict): A dictionary
        containing session, year,
        viewstate, viewstategenerator, and export button information fetched in
        previous steps.

    Returns:
        None
    """

    logger = dg.get_dagster_logger(
        "ak_opening_campaign_disclosure_form_form_export_data"
    )
    year = int(context.partition_key)

    session = ak_search_campaign_disclosure_form_table_data["session"]
    viewstate = ak_search_campaign_disclosure_form_table_data["viewstate"]
    viewstategenerator = ak_search_campaign_disclosure_form_table_data[
        "viewstategenerator"
    ]
    export_btn_name = ak_search_campaign_disclosure_form_table_data["export_btn_name"]

    payload = ak_get_campaign_disclosure_form_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CAMPAIGN_DISCLOSURE_FORM_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CAMPAIGN_DISCLOSURE_FORM_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "campaign_disclosure_form")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_campaign_disclosure_form.csv"

    data = session.get(
        url=AK_CAMPAIGN_DISCLOSURE_FORM_DOWNLOAD_URL, headers=HEADERS, stream=True
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for\
                ak_opening_campaign_disclosure_form_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_campaign_disclosure_income_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_campaign_disclosure_income_export_data(
    context: dg.AssetExecutionContext,
    ak_search_campaign_disclosure_income_table_data: dict,
):
    """
    This function opens the campaign disclosure income export data for a specific year.

    It retrieves income form data for the year and sends a POST request
    to the Alaska State campaign
    website to initiate the data export. Once the data is ready,
    the function downloads the CSV file and
    saves it to a specified directory, with the file named based on the year
    of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the
    necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
    `campaign_disclosure_income`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_campaign_disclosure_income_table_data (dict): A dictionary
        containing session, year,
        viewstate, viewstategenerator, and export button information fetched in
        previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger("ak_opening_campaign_disclosure_income_export_data")
    year = int(context.partition_key)

    session = ak_search_campaign_disclosure_income_table_data["session"]
    viewstate = ak_search_campaign_disclosure_income_table_data["viewstate"]
    viewstategenerator = ak_search_campaign_disclosure_income_table_data[
        "viewstategenerator"
    ]
    export_btn_name = ak_search_campaign_disclosure_income_table_data["export_btn_name"]

    payload = ak_get_campaign_disclosure_income_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CAMPAIGN_DISCLOSURE_INCOME_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CAMPAIGN_DISCLOSURE_INCOME_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "campaign_disclosure_income")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_campaign_disclosure_income.csv"

    data = session.get(
        url=AK_CAMPAIGN_DISCLOSURE_INCOME_DOWNLOAD_URL, headers=HEADERS, stream=True
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for\
                ak_opening_campaign_disclosure_income_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_campaign_disclosure_expenditures_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_campaign_disclosure_expenditures_form_export_data(
    context: dg.AssetExecutionContext,
    ak_search_campaign_disclosure_expenditures_table_data: dict,
):
    """
    This function opens the campaign disclosure expenditures export data
    for a specific year.

    It retrieves expenditures form data for the year and sends a POST request
    to the Alaska State campaign
    website to initiate the data export. Once the data is ready,
    the function downloads the CSV file and
    saves it to a specified directory, with the file named based on the
    year of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
    `campaign_disclosure_expenditures`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_campaign_disclosure_expenditures_table_data (dict):
        A dictionary containing session, year,
        viewstate, viewstategenerator, and export button information fetched in
        previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger(
        "ak_opening_campaign_disclosure_expenditures_form_export_data"
    )
    year = int(context.partition_key)

    session = ak_search_campaign_disclosure_expenditures_table_data["session"]
    viewstate = ak_search_campaign_disclosure_expenditures_table_data["viewstate"]
    viewstategenerator = ak_search_campaign_disclosure_expenditures_table_data[
        "viewstategenerator"
    ]
    export_btn_name = ak_search_campaign_disclosure_expenditures_table_data[
        "export_btn_name"
    ]
    payload = ak_get_campaign_disclosure_expenditures_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "campaign_disclosure_expenditures")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_campaign_disclosure_expenditures.csv"

    data = session.get(
        url=AK_CAMPAIGN_DISCLOSURE_EXPENDITURES_DOWNLOAD_URL,
        headers=HEADERS,
        stream=True,
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for \
                ak_opening_campaign_disclosure_expenditures_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_disclosure_transations_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_disclosure_transations_form_export_data(
    context: dg.AssetExecutionContext,
    ak_search_disclosure_transations_table_data: dict,
):
    """
    This function opens the campaign disclosure transactions export data
    for a specific year.

    It retrieves transactions form data for the year and sends a
    POST request to the Alaska State campaign
    website to initiate the data export. Once the data is ready,
    the function downloads the CSV file and
    saves it to a specified directory, with the file named based
    on the year of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with
    the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
    `campaign_disclosure_transactions`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_disclosure_transations_table_data (dict): A dictionary
        containing session, year,
        viewstate, viewstategenerator, and export button information fetched
        in previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger("ak_opening_disclosure_transations_form_export_data")
    year = int(context.partition_key)

    session = ak_search_disclosure_transations_table_data["session"]
    viewstate = ak_search_disclosure_transations_table_data["viewstate"]
    viewstategenerator = ak_search_disclosure_transations_table_data[
        "viewstategenerator"
    ]
    export_btn_name = ak_search_disclosure_transations_table_data["export_btn_name"]

    payload = ak_get_campaign_disclosure_transations_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "campaign_disclosure_transactions")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_campaign_disclosure_transactions.csv"

    data = session.get(
        url=AK_CAMPAIGN_DISCLOSURE_TRANSACTIONS_DOWNLOAD_URL,
        headers=HEADERS,
        stream=True,
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for \
                ak_opening_disclosure_transations_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_campaign_disclosure_debt_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_campaign_disclosure_debt_form_export_data(
    context: dg.AssetExecutionContext,
    ak_search_campaign_disclosure_debt_table_data: dict,
):
    """
    This function opens the campaign disclosure debt export data for a specific year.

    It retrieves debt form data for the year and sends a POST request
    to the Alaska State campaign
    website to initiate the data export. Once the data is ready, the function
    downloads the CSV file and
    saves it to a specified directory, with the file named based on the year
    of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
        `campaign_disclosure_debt`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_campaign_disclosure_debt_table_data (dict): A dictionary
        containing session, year, viewstate, viewstategenerator,
        and export button information fetched in previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger(
        "ak_opening_campaign_disclosure_debt_form_export_data"
    )
    year = int(context.partition_key)

    session = ak_search_campaign_disclosure_debt_table_data["session"]
    viewstate = ak_search_campaign_disclosure_debt_table_data["viewstate"]
    viewstategenerator = ak_search_campaign_disclosure_debt_table_data[
        "viewstategenerator"
    ]
    export_btn_name = ak_search_campaign_disclosure_debt_table_data["export_btn_name"]

    payload = ak_get_campaign_disclosure_debt_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CAMPAIGN_DISCLOSURE_DEBT_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CAMPAIGN_DISCLOSURE_DEBT_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "campaign_disclosure_debt")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_campaign_disclosure_debt.csv"

    data = session.get(
        url=AK_CAMPAIGN_DISCLOSURE_DEBT_DOWNLOAD_URL, headers=HEADERS, stream=True
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for \
            ak_opening_campaign_disclosure_debt_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


@dg.asset(
    deps=[ak_search_contribution_reports_table_data],
    partitions_def=ak_yearly_partition,
)
def ak_opening_contribution_reports_form_export_data(
    context: dg.AssetExecutionContext,
    ak_search_contribution_reports_table_data: dict,
):
    """
    This function opens the contribution reports export data for a specific year.

    It retrieves contribution reports form data for the year and sends a POST request
    to the Alaska State campaign
    website to initiate the data export. Once the data is ready, the function
    downloads the CSV file and
    saves it to a specified directory, with the file named based on the year
    of the data.

    The function operates in the following sequence:
    1. Gets the year from the partition context.
    2. Sends a POST request to the Alaska campaign website with the necessary form data.
    3. Downloads the exported CSV file.
    4. Saves the downloaded data into a local directory structure under
        `contribution_reports`.

    Args:
        context (dg.AssetExecutionContext): Dagster context containing partition info
        ak_search_contribution_reports_table_data (dict): A dictionary
        containing session, year, viewstate, viewstategenerator,
        and export button information fetched in previous steps.

    Returns:
        None
    """
    logger = dg.get_dagster_logger("ak_opening_contribution_reports_form_export_data")
    year = int(context.partition_key)

    session = ak_search_contribution_reports_table_data["session"]
    viewstate = ak_search_contribution_reports_table_data["viewstate"]
    viewstategenerator = ak_search_contribution_reports_table_data["viewstategenerator"]
    export_btn_name = ak_search_contribution_reports_table_data["export_btn_name"]

    payload = ak_get_contribution_reports_form_data(
        year=year,
        viewstate=str(viewstate),
        viewstategenerator=str(viewstategenerator),
        export_btn_name=export_btn_name,
    )

    r = session.post(
        url=AK_CONTRIBUTION_REPORTS_URL,
        data=payload,
        headers={
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": AK_CONTRIBUTION_REPORTS_URL,
            "Origin": "https://aws.state.ak.us",
        },
    )

    data_dir = Path(AK_DATA_PATH_PREFIX, "contribution_reports")
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / f"{year}_contribution_reports.csv"

    data = session.get(
        url=AK_CONTRIBUTION_REPORTS_DOWNLOAD_URL, headers=HEADERS, stream=True
    )

    with open(csv_file_path, "wb") as f:
        for chunk in data.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(
        "Successfully Download Data for \
            ak_opening_contribution_reports_form_export_data"
    )
    logger.info(f"Year: {year} Download status: {r.status_code} in {csv_file_path}")


####################################################

"""
Assets For Inserting CSV Export Data Request
"""


@dg.asset(
    deps=[ak_opening_all_candidates_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_all_candidates_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting the Ak All Candidates data for a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL("DELETE FROM {table_name} WHERE year = {year_str}").format(
        table_name=sql.Identifier("ak_all_candidates_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_all_candidates_landing",
        table_columns_name=[
            "result",
            "year",
            "candidate",
            "candidate_email",
            "address",
            "city",
            "state_region",
            "zip",
            "country",
            "office",
            "election",
            "source",
            "won",
            "status",
            "party",
            "treasurer",
            "treasurer_email",
            "chair",
            "chair_email",
            "initial_filing",
        ],
        data_validation_callback=lambda row: len(row) == 20,
        category="all_candidates",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_campaign_disclosure_form_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_campaign_disclosure_form_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting the Campaign Disclosure Form data for a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_campaign_disclosure_form_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_campaign_disclosure_form_landing",
        table_columns_name=[
            "result",
            "report_year",
            "report_type",
            "begin_date",
            "end_date",
            "filer_type",
            "name",
            "beginning_cash_on_hand",
            "total_income",
            "previous_campaign_income",
            "campaign_income_total",
            "total_expenditures",
            "previous_campaign_expense",
            "campaign_expense_total",
            "closing_cash_on_hand",
            "total_debt",
            "surplus_deficit",
            "submitted",
            "status",
            "amending",
        ],
        data_validation_callback=lambda row: len(row) == 20,
        category="campaign_disclosure_form",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_campaign_disclosure_income_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_campaign_disclosure_income_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Campaign Disclosure Income data for a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_campaign_disclosure_income_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_campaign_disclosure_income_landing",
        table_columns_name=[
            "result",
            "date",
            "transaction_type",
            "payment_type",
            "payment_detail",
            "amount",
            "last_business_name",
            "first_name",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "occupation",
            "employer",
            "purpose_of_expenditure",
            "placeholder",
            "report_type",
            "election_name",
            "election_type",
            "municipality",
            "office",
            "filer_type",
            "name",
            "report_year",
            "submitted",
        ],
        data_validation_callback=lambda row: len(row) == 26,
        category="campaign_disclosure_income",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_campaign_disclosure_expenditures_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_campaign_disclosure_expenditures_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Campaign Disclosure Expenditures data for a specific
    year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_campaign_disclosure_expenditures_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_campaign_disclosure_expenditures_landing",
        table_columns_name=[
            "result",
            "date",
            "transaction_type",
            "payment_type",
            "payment_detail",
            "amount",
            "last_business_name",
            "first_name",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "occupation",
            "employer",
            "purpose_of_expenditure",
            "placeholder",
            "report_type",
            "election_name",
            "election_type",
            "municipality",
            "office",
            "filer_type",
            "name",
            "report_year",
            "submitted",
        ],
        data_validation_callback=lambda row: len(row) == 26,
        category="campaign_disclosure_expenditures",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_disclosure_transations_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_campaign_disclosure_transactions_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Campaign Disclosure Transactions data for
    a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_campaign_disclosure_transactions_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_campaign_disclosure_transactions_landing",
        table_columns_name=[
            "result",
            "date",
            "transaction_type",
            "payment_type",
            "payment_detail",
            "amount",
            "last_business_name",
            "first_name",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "occupation",
            "employer",
            "purpose_of_expenditure",
            "placeholder_column",
            "report_type",
            "election_name",
            "election_type",
            "municipality",
            "office",
            "filer_type",
            "name",
            "report_year",
            "submitted",
        ],
        data_validation_callback=lambda row: len(row) == 26,
        category="campaign_disclosure_transactions",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_campaign_disclosure_debt_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_campaign_disclosure_debt_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Campaign Disclosure Debt data for a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_campaign_disclosure_debt_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_campaign_disclosure_debt_landing",
        table_columns_name=[
            "result",
            "date",
            "balance_remaining",
            "original_amount",
            "name",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "description_purpose",
            "placeholder_column",
            "filer_type",
            "name_2",
            "report_year",
            "submitted",
        ],
        data_validation_callback=lambda row: len(row) == 16,
        category="campaign_disclosure_debt",
        year=year,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[ak_opening_contribution_reports_form_export_data],
    partitions_def=ak_yearly_partition,
)
def ak_inserting_contribution_reports_data(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Contribution Reports data for a specific year to landing table.
    """
    year = int(context.partition_key)

    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE report_year = {year_str}"
    ).format(
        table_name=sql.Identifier("ak_contribution_reports_landing"),
        year_str=sql.Literal(str(year)),
    )

    return ak_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ak_contribution_reports_landing",
        table_columns_name=[
            "result",
            "report_year",
            "filing_reason",
            "filer_type",
            "filer_name",
            "filer_address",
            "filer_city",
            "filer_state",
            "filer_zip",
            "filer_country",
            "business_type",
            "email",
            "phone",
            "occupation",
            "employer",
            "total_contributed_this_report",
            "total_contributed_this_year",
            "submitted",
            "status",
            "separator",
            "contribution_count",
            "contact_first_name",
            "contact_last_name",
            "contact_email",
            "contact_phone",
        ],
        data_validation_callback=lambda row: len(row) == 25,
        category="contribution_reports",
        year=year,
        truncate_query=truncate_query,
    )

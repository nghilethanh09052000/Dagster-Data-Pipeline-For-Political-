import csv
import os
import re
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
import requests
from bs4 import BeautifulSoup
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Status fields for Alabama
# 1 is Activate, 4 is Dissolved
AL_STATUS_FIELDS = ["1", "4"]


AL_FIRST_YEAR_DATA_AVAILABLE = datetime(year=2013, month=1, day=1)

AL_DATA_PATH_PREFIX = "./states/alabama"
AL_BASE_URL = "https://fcpa.alabamavotes.gov/PublicSite/Docs/BulkDataDownloads"
AL_CANDIDATE_URL = (
    "https://fcpa.alabamavotes.gov/PublicSite/SearchPages/CandidateSearch.aspx"
)
AL_PAC_URL = "https://fcpa.alabamavotes.gov/PublicSite/SearchPages/CommitteeSearch.aspx?tb=pacsearch"

AL_COMMITTEE_DETAILS_URL = (
    "https://fcpa.alabamavotes.gov/PublicSite/SearchPages/CommitteeDetail.aspx?OrgID="
)
CATEGORY_MAPPING = {
    "CashContributionsExtract": "al_cash_contributions",
    "ExpendituresExtract": "al_expenditures",
    "InKindContributionsExtract": "al_in_kind_contributions",
    "OtherReceiptsExtract": "al_other_receipts",
}
HEADERS = {
    "accept": "text/html,\
            application/xhtml+xml,\
            application/xml;q=0.9,\
            image/avif,image/webp,\
            image/apng,*/*;q=0.8,\
            application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "referer": "https://fcpa.alabamavotes.gov/PublicSite/DataDownload.aspx?tb=downloaddata",
    "sec-ch-ua": '"Chromium";v="134", \
                "Not:A-Brand";v="24", \
                "Microsoft Edge";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh;\
        Intel Mac OS X 10_15_7) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
}
CANDIDATES_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
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

AL_CANDIDATE_FIELDS = [
    "candidate_name",
    "party",
    "office",
    "district",
    "place",
    "committee_status",
    "committee_id",
    "address",
    "committee_type",
    "phone",
    "date_registered",
    "jurisdiction",
]

AL_PAC_COMMITTEE_FIELDS = [
    "committee_name",
    "city",
    "committee_status",
    "committee_id",
    "address",
    "committee_type",
    "phone",
    "date_registered",
    "jurisdiction",
]


def al_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    category: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    data_files: list[str],
    truncate_query: sql.Composed | None = None,
):
    """
    Insert raw AL-ACCESS data to landing table in Postgres.

    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.

    category: the CATEGORY_MAPPING we use to check to get the correct file path

    table_name: the name of the table that's going to be used for ingesting

    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for al_cash_contributions.
                        ["OrgID","ContributionAmount","ContributionDate",
                         "CommitteeName", ..., "Amended"]

    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs

    data_files: the file paths of the actual data file on the year folder and
                    category folders, so that we will have corrent path and
                    use it to ingest to table
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    if not data_files:
        logger.warning(f"No data files found for {category}, skipping insertion.")
        return

    logger.info(
        f"Process With Category: {category}\
                Data Files: {data_files}"
    )

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(
            f"Truncating table {table_name} before inserting new data.\
                with query: {truncate_query}"
        )
        pg_cursor.execute(
            truncate_query
            if truncate_query is not None
            else get_sql_truncate_query(table_name=table_name)
        )

        for data_file in data_files:
            if not data_file or not os.path.exists(data_file):
                logger.warning(
                    f"No data files found on {data_file}, skipping insertion"
                )
                continue

            try:
                logger.info(f"Processing file: {data_file}")

                data_type_lines_generator = safe_readline_csv_like_file(
                    data_file,
                    encoding="utf-8",
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=",", quotechar='"'
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
                logger.error(f"Error while processing {data_file}: {e}")

            # Get row count after insertion
            count_query = get_sql_count_query(table_name=table_name)
            count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
            row_count = int(count_cursor_result[0]) if count_cursor_result else 0

            logger.info(f"Successfully inserted {row_count} rows into {table_name}")

            return dg.MaterializeResult(
                metadata={
                    "dagster/table_name": table_name,
                    "dagster/row_count": row_count,
                }
            )


def al_extract_hidden_input_value(html_content, key):
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


def get_view_state_data(html_content):
    return {
        "__VIEWSTATE": (al_extract_hidden_input_value(html_content, "__VIEWSTATE")),
        "__VIEWSTATEGENERATOR": (
            al_extract_hidden_input_value(html_content, "__VIEWSTATEGENERATOR")
        ),
        "__SCROLLPOSITIONX": (
            al_extract_hidden_input_value(html_content, "__SCROLLPOSITIONX")
        ),
        "__SCROLLPOSITIONY": (
            al_extract_hidden_input_value(html_content, "__SCROLLPOSITIONY")
        ),
        "__EVENTVALIDATION": (
            al_extract_hidden_input_value(html_content, "__EVENTVALIDATION")
        ),
    }


def extract_max_page_number(html: str) -> int:
    """Extracts the maximum page number from pagination links."""
    soup = BeautifulSoup(html, "html.parser")
    page_links = soup.select(
        "a[id^='_ctl0_Content_dgdSearchResults__ctl13_dgdSearchResultsPageLink']"
    )
    page_numbers = [
        int(link.get_text(strip=True))
        for link in page_links
        if link.get_text(strip=True).isdigit()
    ]
    return max(page_numbers) if page_numbers else 1


def make_request(session, payload, url):
    return session.post(url=url, headers=CANDIDATES_HEADERS, data=payload)


def get_committee_details_data(session, committee_id):
    url = f"{AL_COMMITTEE_DETAILS_URL}{committee_id}"
    print(f"Fetching committee details from {url}")
    html_content = session.get(url=url, headers=HEADERS)
    return html_content.text


"""
Alabama fetch
election finance contributions
"""
al_schedule_yearly_partition = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=AL_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(partitions_def=al_schedule_yearly_partition)
def al_fetch_contributions_bulk_raw_data(context: dg.AssetExecutionContext):
    """
    Fetches bulk data on this API:
    https://fcpa.alabamavotes.gov/PublicSite/Docs/BulkDataDownloads

    Based on different 4 categories:
        - CashContributionsExtract
        - ExpendituresExtract
        - InKindContributionsExtract
        - OtherReceiptsExtract

    From this website:
    https://fcpa.alabamavotes.gov/PublicSite/DataDownload.aspx?tb=downloaddata

    This asset will download and unzip to csv
    """

    logger = dg.get_dagster_logger(name="al_fetch_contributions_bulk_raw_data")
    year = int(context.partition_key)

    for category in CATEGORY_MAPPING:
        url = f"{AL_BASE_URL}/{year}_{category}.csv.zip"
        response = requests.head(url, headers=HEADERS)
        if response.status_code != 200:
            logger.info(f"No data available for {category} in {year}")
            continue

        logger.info(f"Fetching data for {category} - {year}...")
        year_dir = Path(AL_DATA_PATH_PREFIX, str(year), category)
        year_dir.mkdir(parents=True, exist_ok=True)
        zip_file_path = year_dir / f"{year}_{category}.zip"
        extracted_file_path = year_dir / f"{year}_{category}.csv"

        stream_download_file_to_path(
            request_url=url, file_save_path=zip_file_path, headers=HEADERS
        )

        try:
            with ZipFile(zip_file_path, "r") as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith(".csv"):
                        with (
                            zip_ref.open(file_info) as source,
                            open(extracted_file_path, "wb") as target,
                        ):
                            shutil.copyfileobj(source, target, 65536)
        finally:
            os.remove(zip_file_path)


@dg.asset(
    deps=[al_fetch_contributions_bulk_raw_data],
    partitions_def=al_schedule_yearly_partition,
)
def al_insert_cash_contributions_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting all the Cash Contributions data to landing table.
    """

    category = "CashContributionsExtract"
    year = int(context.partition_key)
    table_name = CATEGORY_MAPPING[category]
    year_dir = Path(AL_DATA_PATH_PREFIX, str(year), category)
    data_file = str(year_dir / f"{year}_{category}.csv")
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE SUBSTRING(contribution_date, 1, 4) = {year}"
    ).format(table_name=sql.Identifier(table_name), year=sql.Literal(str(year)))

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category=category,
        table_name=table_name,
        table_columns_name=[
            "org_id",
            "contribution_amount",
            "contribution_date",
            "last_name",
            "first_name",
            "mi",
            "suffix",
            "address1",
            "city",
            "state",
            "zip",
            "contribution_id",
            "filed_date",
            "contribution_type",
            "contributor_type",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
        ],
        data_validation_callback=lambda row: len(row) == 19,
        data_files=[data_file],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[al_fetch_contributions_bulk_raw_data],
    partitions_def=al_schedule_yearly_partition,
)
def al_insert_expenditures_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting all the Expenditures data to landing table.
    """
    category = "ExpendituresExtract"
    year = int(context.partition_key)
    table_name = CATEGORY_MAPPING[category]
    year_dir = Path(AL_DATA_PATH_PREFIX, str(year), category)
    data_file = str(year_dir / f"{year}_{category}.csv")
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE SUBSTRING(expenditure_date, 1, 4) = {year}"
    ).format(table_name=sql.Identifier(table_name), year=sql.Literal(str(year)))

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category=category,
        table_name=table_name,
        table_columns_name=[
            "org_id",
            "expenditure_amount",
            "expenditure_date",
            "last_name",
            "first_name",
            "mi",
            "suffix",
            "address1",
            "city",
            "state",
            "zip",
            "explanation",
            "expenditure_id",
            "filed_date",
            "purpose",
            "expenditure_type",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
        ],
        data_validation_callback=lambda row: len(row) == 20,
        data_files=[data_file],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[al_fetch_contributions_bulk_raw_data],
    partitions_def=al_schedule_yearly_partition,
)
def al_insert_in_kind_contributions_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting all the In-Kind Contributions data to landing table.
    """
    category = "InKindContributionsExtract"
    year = int(context.partition_key)
    table_name = CATEGORY_MAPPING[category]
    year_dir = Path(AL_DATA_PATH_PREFIX, str(year), category)
    data_file = str(year_dir / f"{year}_{category}.csv")
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE SUBSTRING(contribution_date, 1, 4) = {year}"
    ).format(table_name=sql.Identifier(table_name), year=sql.Literal(str(year)))

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category=category,
        table_name=table_name,
        table_columns_name=[
            "org_id",
            "contribution_amount",
            "contribution_date",
            "last_name",
            "first_name",
            "mi",
            "suffix",
            "address1",
            "city",
            "state",
            "zip",
            "in_kind_contribution_id",
            "filed_date",
            "contribution_type",
            "contributor_type",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
            "nature_of_in_kind_contribution",
        ],
        data_validation_callback=lambda row: len(row) == 20,
        data_files=[data_file],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[al_fetch_contributions_bulk_raw_data],
    partitions_def=al_schedule_yearly_partition,
)
def al_insert_other_receipts_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting all Other Receipts data to landing table.
    """
    category = "OtherReceiptsExtract"
    year = int(context.partition_key)
    table_name = CATEGORY_MAPPING[category]
    year_dir = Path(AL_DATA_PATH_PREFIX, str(year), category)
    data_file = str(year_dir / f"{year}_{category}.csv")
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE SUBSTRING(receipt_date, 1, 4) = {year}"
    ).format(table_name=sql.Identifier(table_name), year=sql.Literal(str(year)))

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category=category,
        table_name=table_name,
        table_columns_name=[
            "org_id",
            "receipt_amount",
            "receipt_date",
            "last_name",
            "first_name",
            "mi",
            "suffix",
            "address1",
            "city",
            "state",
            "zip",
            "receipt_id",
            "filed_date",
            "receipt_type",
            "receipt_source_type",
            "committee_type",
            "committee_name",
            "candidate_name",
            "amended",
            "endorser_name1",
            "endorser_address1",
            "endorser_guaranteed_amt1",
            "endorser_name2",
            "endorser_address2",
            "endorser_guaranteed_amt2",
            "endorser_name3",
            "endorser_address3",
            "endorser_guaranteed_amt3",
        ],
        data_validation_callback=lambda row: len(row) == 28,
        data_files=[data_file],
        truncate_query=truncate_query,
    )


"""
Alabama fetch
election finance candidates and committees
"""

al_schedule_static_status_partition = dg.StaticPartitionsDefinition(
    partition_keys=AL_STATUS_FIELDS
)


@dg.asset(partitions_def=al_schedule_static_status_partition)
def al_fetch_candidates_data(context: dg.AssetExecutionContext):
    """
    Fetches Alabama campaign committee data by automating
    form submission and pagination
    through ASP.NET web interface.
    Stores the data as a CSV file
    """
    logger = dg.get_dagster_logger("al_fetch_candidates_data")
    session = requests.session()
    status = context.partition_key

    def extract_candidate_data(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr.GridItem, tr.GridAlternatingItem")
        candidates = []

        for row in rows:
            cols = row.find_all("td")
            candidates.append(
                {
                    "candidate_name": cols[0].get_text(strip=True),
                    "party": cols[1].get_text(strip=True),
                    "office": cols[2].get_text(strip=True),
                    "district": cols[3].get_text(strip=True),
                    "place": cols[4].get_text(strip=True),
                    "committee_status": cols[5].get_text(strip=True),
                    "committee_id": cols[6].get_text(strip=True),
                }
            )
        return candidates

    def extract_candidate_details(html: str) -> dict:
        """Extract candidate committee details from the committee details page."""
        soup = BeautifulSoup(html, "html.parser")
        details = {
            "address": "",
            "committee_type": "",
            "phone": "",
            "date_registered": "",
            "jurisdiction": "",
        }

        # Extract address (may span multiple <span> tags)
        address_spans = soup.select(
            "#_ctl0_Content_lblPhysAddress1, #_ctl0_Content_lblPhysCityStateZip"
        )
        address_parts = [span.get_text(strip=True) for span in address_spans]
        details["address"] = " ".join(address_parts)

        # Extract committee type
        ct_span = soup.select_one("#_ctl0_Content_lblCommitteeType")
        if ct_span:
            details["committee_type"] = ct_span.get_text(strip=True)

        # Extract phone
        phone_span = soup.select_one("#_ctl0_Content_lblCommPhone")
        if phone_span:
            details["phone"] = phone_span.get_text(strip=True)

        # Extract date registered
        date_span = soup.select_one("#_ctl0_Content_lblCommDateOrganized")
        if date_span:
            details["date_registered"] = date_span.get_text(strip=True)

        # Extract jurisdiction
        juris_span = soup.select_one("#_ctl0_Content_lblCommDistrict")
        if juris_span:
            details["jurisdiction"] = juris_span.get_text(strip=True)

        return details

    # First GET
    response = session.get(AL_CANDIDATE_URL, headers=CANDIDATES_HEADERS)
    html_content = response.text
    view_state_data = get_view_state_data(html_content)

    # First search POST
    payload = {
        "_ctl0_ToolkitScriptManager1_HiddenField": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": view_state_data["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": view_state_data["__VIEWSTATEGENERATOR"],
        "__SCROLLPOSITIONX": view_state_data["__SCROLLPOSITIONX"],
        "__SCROLLPOSITIONY": view_state_data["__SCROLLPOSITIONY"],
        "__EVENTVALIDATION": view_state_data["__EVENTVALIDATION"],
        "_ctl0:Content:txtLastName": "",
        "_ctl0:Content:rblLastNameSearchType": "1",
        "_ctl0:Content:txtFirstName": "",
        "_ctl0:Content:rblFirstNameSearchType": "1",
        "_ctl0:Content:txtCandidateID": "",
        "_ctl0:Content:ddlParty": "ucddlParty: -1",
        "_ctl0:Content:ddlOffice": "Select all offices...",
        "_ctl0:Content:ddlStatus": status,
        "_ctl0:Content:btnSearch": "Search",
    }

    response = make_request(session=session, payload=payload, url=AL_CANDIDATE_URL)
    html_content = response.text
    view_state_data = get_view_state_data(html_content)
    all_candidates = extract_candidate_data(html_content)
    max_page = extract_max_page_number(html_content)

    # Fetch committee details for first page candidates
    for candidate in all_candidates:
        committee_id = candidate.get("committee_id", "")
        logger.info(f"Fetching details for candidate ID: {committee_id}")
        html = get_committee_details_data(session=session, committee_id=committee_id)
        details = extract_candidate_details(html)
        candidate.update(details)

    i = 1
    while True:
        if i > max_page:
            break

        i += 1

        pagination_payload = {
            "_ctl0_ToolkitScriptManager1_HiddenField": "",
            "__EVENTTARGET": (
                f"_ctl0:Content:dgdSearchResults:_ctl13:dgdSearchResultsPageLink{i}"
            ),
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            **view_state_data,
            "_ctl0:Content:dgdSearchResults:_ctl13:dgdSearchResultsPageSizeDropDown": (
                "10"
            ),
        }

        response = make_request(
            session=session, payload=pagination_payload, url=AL_CANDIDATE_URL
        )

        html_content = response.text

        view_state_data = get_view_state_data(html_content)
        page_candidates = extract_candidate_data(html_content)

        for page_candidate in page_candidates:
            committee_id = page_candidate.get("committee_id", "")

            view_state_data = get_view_state_data(html_content)

            payload = {
                "_ctl0_ToolkitScriptManager1_HiddenField": "",
                "__EVENTTARGET": ("_ctl0$Content$dgdSearchResults$_ctl2$lnkCandidate"),
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                **view_state_data,
                "_ctl0:Content:dgdSearchResults:"
                "_ctl13:dgdSearchResultsPageSizeDropDown": ("10"),
            }

            logger.info(f"Fetching details for candidate ID: {committee_id}")
            html = get_committee_details_data(
                session=session, committee_id=committee_id
            )

            details = extract_candidate_details(html)
            page_candidate.update(details)

        if not page_candidates:
            logger.info(f"No more data on page {i}, breaking loop.")
            break

        print(f"Page {i} candidates: {page_candidates}")

        all_candidates.extend(page_candidates)
        logger.info(f"Fetched page {i} with {page_candidates} candidates.")

    Path(AL_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)
    csv_filename = f"{AL_DATA_PATH_PREFIX}/candidates/{status}/candidates.csv"

    # Create the directory structure before writing the file
    Path(csv_filename).parent.mkdir(parents=True, exist_ok=True)

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=AL_CANDIDATE_FIELDS)
        writer.writeheader()
        writer.writerows(all_candidates)

    logger.info(f"Total candidates extracted: {len(all_candidates)}")


@dg.asset(partitions_def=al_schedule_static_status_partition)
def al_fetch_political_action_committee_data(
    context: dg.AssetExecutionContext,
):
    """
    Fetches Alabama political action committee (PAC) data by status
    by scraping and paginating the web interface.
    Stores the data in CSV files for each letter.
    """
    logger = dg.get_dagster_logger("al_fetch_political_action_committee_data")
    session = requests.session()
    status = context.partition_key

    def extract_political_action_committee_data(html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr.GridItem, tr.GridAlternatingItem")
        candidates = []

        for row in rows:
            cols = row.find_all("td")
            candidates.append(
                {
                    "committee_name": cols[0].get_text(strip=True),
                    "city": cols[1].get_text(strip=True),
                    "committee_status": cols[3].get_text(strip=True),
                    "committee_id": cols[4].get_text(strip=True),
                }
            )
        return candidates

    def extract_pac_details(html: str) -> dict:
        """Extract PAC committee details from the committee details page."""
        soup = BeautifulSoup(html, "html.parser")
        details = {
            "address": "",
            "committee_type": "",
            "phone": "",
            "date_registered": "",
            "jurisdiction": "",
        }

        # Extract address (may span multiple <span> tags)
        address_spans = soup.select(
            "#_ctl0_Content_lblPhysAddress1, #_ctl0_Content_lblPhysCityStateZip"
        )
        address_parts = [span.get_text(strip=True) for span in address_spans]
        details["address"] = " ".join(address_parts)

        # Extract committee type
        ct_span = soup.select_one("#_ctl0_Content_lblCommitteeType")
        if ct_span:
            details["committee_type"] = ct_span.get_text(strip=True)

        # Extract phone
        phone_span = soup.select_one("#_ctl0_Content_lblCommPhone")
        if phone_span:
            details["phone"] = phone_span.get_text(strip=True)

        # Extract date registered
        date_span = soup.select_one("#_ctl0_Content_lblCommDateOrganized")
        if date_span:
            details["date_registered"] = date_span.get_text(strip=True)

        # Extract jurisdiction
        juris_span = soup.select_one("#_ctl0_Content_lblCommDistrict")
        if juris_span:
            details["jurisdiction"] = juris_span.get_text(strip=True)

        return details

    # First GET
    all_candidates = []

    response = session.get(AL_PAC_URL, headers=CANDIDATES_HEADERS)
    html_content = response.text
    view_state_data = get_view_state_data(html_content)

    payload = {
        "_ctl0_ToolkitScriptManager1_HiddenField": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": view_state_data["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": view_state_data["__VIEWSTATEGENERATOR"],
        "__SCROLLPOSITIONX": view_state_data["__SCROLLPOSITIONX"],
        "__SCROLLPOSITIONY": view_state_data["__SCROLLPOSITIONY"],
        "__EVENTVALIDATION": view_state_data["__EVENTVALIDATION"],
        "_ctl0:Content:txtCommitteeName": "",
        "_ctl0:Content:rblCommitteeNameSearchType": "0",
        "_ctl0:Content:txtCommitteeID": "",
        "_ctl0:Content:txtPurpose": "",
        "_ctl0:Content:rblPurposeSearchType": "1",
        "_ctl0:Content:txtAffiliatedorConnectedOrganization": "",
        "_ctl0:Content:rdoAffiliatedorConnectedOrganization": "1",
        "_ctl0:Content:ddlStatus": status,
        "_ctl0:Content:btnSearch": "Search",
    }

    response = make_request(session=session, payload=payload, url=AL_PAC_URL)
    html_content = response.text

    view_state_data = get_view_state_data(html_content)
    status_candidates = extract_political_action_committee_data(html_content)
    max_page = extract_max_page_number(html_content)

    logger.info(
        f"Found {len(status_candidates)} candidates for status {status} on first page"
    )

    # Fetch committee details for first page PACs
    for pac in status_candidates:
        committee_id = pac.get("committee_id", "")
        logger.info(f"Fetching details for committee ID: {committee_id}")
        html = get_committee_details_data(session=session, committee_id=committee_id)
        details = extract_pac_details(html)
        pac.update(details)

    i = 1
    while True:
        if i > max_page:
            break

        i += 1

        pagination_payload = {
            "_ctl0_ToolkitScriptManager1_HiddenField": "",
            "__EVENTTARGET": (
                f"_ctl0:Content:dgdSearchResults:_ctl13:dgdSearchResultsPageLink{i}"
            ),
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            **view_state_data,
            "_ctl0:Content:dgdSearchResults:_ctl13:"
            "dgdSearchResultsPageSizeDropDown": "10",
        }

        response = make_request(
            session=session, payload=pagination_payload, url=AL_PAC_URL
        )

        html_content = response.text
        view_state_data = get_view_state_data(html_content)

        page_candidates = extract_political_action_committee_data(html_content)

        for page_candidate in page_candidates:
            committee_id = page_candidate.get("committee_id", "")

            view_state_data = get_view_state_data(html_content)

            payload = {
                "_ctl0_ToolkitScriptManager1_HiddenField": "",
                "__EVENTTARGET": ("_ctl0$Content$dgdSearchResults$_ctl2$lnkCandidate"),
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                **view_state_data,
                "_ctl0:Content:dgdSearchResults:"
                "_ctl13:dgdSearchResultsPageSizeDropDown": ("10"),
            }

            logger.info(f"Fetching details for committee ID: {committee_id}")
            html = get_committee_details_data(
                session=session, committee_id=committee_id
            )

            details = extract_pac_details(html)
            page_candidate.update(details)

        if not page_candidates:
            logger.info(f"No more data on page {i}, breaking loop.")
            break

        status_candidates.extend(page_candidates)
        logger.info(
            f"Fetched page {i} with {len(page_candidates)} candidates "
            f"for status {status}."
        )

    logger.info(
        f"Completed processing status {status}. "
        f"Total candidates for this status: {len(status_candidates)}"
    )
    all_candidates.extend(status_candidates)

    logger.info(
        f"Completed processing status {status}. Total candidates: {len(all_candidates)}"
    )

    Path(AL_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)
    csv_filename = f"{AL_DATA_PATH_PREFIX}/committees/{status}/pac.csv"

    # Create the directory structure before writing the file
    Path(csv_filename).parent.mkdir(parents=True, exist_ok=True)

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=AL_PAC_COMMITTEE_FIELDS)
        writer.writeheader()
        writer.writerows(all_candidates)

    logger.info(
        f"Total candidates extracted for status {status}: {len(all_candidates)}"
    )


@dg.asset(
    deps=[al_fetch_candidates_data],
    partitions_def=al_schedule_static_status_partition,
)
def al_insert_candidates_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Inserting Campaign Committee data to landing table.
    """
    status = context.partition_key
    data_file = f"{AL_DATA_PATH_PREFIX}/candidates/{status}/candidates.csv"
    table_name = "al_candidates_landing"
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE committee_status = {status}"
    ).format(
        table_name=sql.Identifier(table_name),
        status=sql.Literal("Active" if status == "1" else "Dissolved"),
    )

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="candidates",
        table_name=table_name,
        table_columns_name=AL_CANDIDATE_FIELDS,
        data_validation_callback=(lambda row: len(row) == len(AL_CANDIDATE_FIELDS)),
        data_files=[data_file],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[al_fetch_political_action_committee_data],
    partitions_def=al_schedule_static_status_partition,
)
def al_insert_political_action_committee_to_landing_table(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting Political Action Committee data to landing table.
    """
    status = context.partition_key
    data_file = f"{AL_DATA_PATH_PREFIX}/committees/{status}/pac.csv"
    table_name = "al_political_action_committee_landing"
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE committee_status = {status}"
    ).format(
        table_name=sql.Identifier(table_name),
        status=sql.Literal("Active" if status == "1" else "Dissolved"),
    )

    return al_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        category="political_action_committee",
        table_name=table_name,
        table_columns_name=AL_PAC_COMMITTEE_FIELDS,
        data_validation_callback=(lambda row: len(row) == len(AL_PAC_COMMITTEE_FIELDS)),
        data_files=[data_file],
        truncate_query=truncate_query,
    )

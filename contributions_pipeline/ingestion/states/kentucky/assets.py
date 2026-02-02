import csv
import glob
import os
from collections.abc import Callable
from pathlib import Path
from urllib.parse import quote_plus, urljoin

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

KY_DATA_PATH_PREFIX = "./states/kentucky"
KY_ELECTION_DATES_URL = (
    "https://secure.kentucky.gov/kref/publicsearch/GetAllElectionDates"
)

KY_CANDIDATES_SEARCH_URL = (
    "https://secure.kentucky.gov/kref/publicsearch/CandidateSearch"
)

KY_ORGANIZATIONS_SEARCH_URL = (
    "https://secure.kentucky.gov/kref/publicsearch/Home/ExportOrganizationSearch"
)

KY_CONTRIBUTIONS_SEARCH_URL = (
    "https://secure.kentucky.gov/kref/publicsearch/AllContributors"
)
KY_EXPENDITURES_SEARCH_URL = (
    "https://secure.kentucky.gov/kref/publicsearch/GetExpenditure"
)


KY_BASE_URL = "https://secure.kentucky.gov"


def fetch_ky_election_dates() -> list[str]:
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": KY_CANDIDATES_SEARCH_URL,
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        ),
    }
    response = requests.get(KY_ELECTION_DATES_URL, headers=headers)
    response.raise_for_status()
    return response.json()


def ky_insert_raw_file_to_landing_table(
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

    base_path = Path(KY_DATA_PATH_PREFIX)
    data_files = []

    glob_path = base_path / category

    if glob_path.exists():
        files = glob.glob(str(glob_path / f"{category}_*.csv"))
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
def ky_fetch_candidates_data():
    """
    Fetches candidate data for each election date in Kentucky.

    This function iterates over the election dates, constructs a search URL for
    each date, and sends requests to retrieve the candidate data. If the data is
    available, it downloads the CSV file containing the candidate data and saves
    it to a local directory for further processing.

    Steps:
    1. Retrieve election dates.
    2. Construct the search URL for each election date.
    3. Fetch the search results page and locate the export link.
    4. Download the corresponding CSV file.
    5. Save the CSV to the appropriate directory.

    The function logs key events including successes, failures, and missing
        export links.

    Args:
        None

    Returns:
        None
    """
    election_dates = fetch_ky_election_dates()
    logger = dg.get_dagster_logger(name="ky_fetch_candidates_data")
    name = "candidates"
    for election_date in election_dates:
        logger.info(f"Processing election date: {election_date}")
        encoded_date = quote_plus(election_date)

        search_url = (
            f"{KY_CANDIDATES_SEARCH_URL}"
            f"?FirstName=&LastName=&ElectionDate={encoded_date}"
            f"&PoliticalParty=&ExemptionStatus=All&IsActiveFlag="
        )

        headers = {
            "Referer": KY_CANDIDATES_SEARCH_URL,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
            ),
        }

        try:
            response = requests.get(search_url, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to load CandidateSearch for {election_date}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        export_link_tag = soup.find("a", title="Export search results")

        if isinstance(export_link_tag, Tag) and export_link_tag.get("href"):
            export_path = str(export_link_tag["href"])
            export_url = urljoin(KY_BASE_URL, export_path)

            base_dir = Path(KY_DATA_PATH_PREFIX).joinpath(name)
            base_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{name}_{election_date.replace('/', '-')}.csv"
            file_path = os.path.join(base_dir, file_name)

            stream_download_file_to_path(
                request_url=export_url,
                file_save_path=file_path,
                headers=headers,
            )

            logger.info(f"Saved CSV for {election_date} to {file_path}")


@dg.asset
def ky_fetch_organizations_data():
    """
    Fetches candidate data for each election date in Kentucky.

    This function iterates over the election dates, constructs a search URL for
    each date, and sends requests to retrieve the candidate data. If the data is
    available, it downloads the CSV file containing the candidate data and saves
    it to a local directory for further processing.

    Steps:
    1. Retrieve election dates.
    2. Construct the search URL for each election date.
    3. Fetch the search results page and locate the export link.
    4. Download the corresponding CSV file.
    5. Save the CSV to the appropriate directory.

    The function logs key events including successes, failures, and missing
        export links.

    Args:
        None

    Returns:
        None
    """
    logger = dg.get_dagster_logger(name="ky_fetch_organizations_data")
    name = "organizations"

    logger.info("Processing Contributions Fetching...")

    base_dir = Path(KY_DATA_PATH_PREFIX).joinpath(name)
    base_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{name}_all.csv"
    file_path = os.path.join(base_dir, file_name)

    headers = {
        "Referer": KY_ORGANIZATIONS_SEARCH_URL,
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        ),
    }

    stream_download_file_to_path(
        request_url=KY_ORGANIZATIONS_SEARCH_URL,
        file_save_path=file_path,
        headers=headers,
    )

    logger.info(f"Saved CSV for contributions to {file_path}")

    # try:
    #     response = requests.get(search_url, headers=headers)
    #     response.raise_for_status()
    # except requests.RequestException as e:
    #     logger.warning(f"Failed to load CandidateSearch for {election_date}: {e}")
    #     continue

    # soup = BeautifulSoup(response.text, "html.parser")
    # export_link_tag = soup.find("a", title="Export search results")

    # if isinstance(export_link_tag, Tag) and export_link_tag.get("href"):


@dg.asset
def ky_fetch_contributions_data():
    """
    Fetches campaign contributions data for each election date in Kentucky.

    This function iterates over election dates, constructs a search URL for each
    date, sends requests to retrieve contributions data, and downloads the
    CSV containing the contributions data. The data is saved to a local directory
    for further processing.

    Steps:
    1. Retrieve election dates.
    2. Construct search URL for each election date.
    3. Fetch search results page and locate the export link.
    4. Download the corresponding CSV file.
    5. Save the CSV to the appropriate directory.

    The function logs key events including successes, failures, and missing
        export links.
    """

    election_dates = fetch_ky_election_dates()
    logger = dg.get_dagster_logger(name="ky_fetch_contributions_data")
    name = "contributions"

    for election_date in election_dates:
        logger.info(f"Processing election date: {election_date}")
        encoded_date = quote_plus(election_date)

        search_url = (
            f"{KY_CONTRIBUTIONS_SEARCH_URL}"
            f"?FirstName=&LastName=&FromOrganizationName=&ElectionDate={encoded_date}"
            f"&ElectionType=&City=&State=&Zip=&Employer=&Occupation=&OtherOccupation="
            f"&MinAmount=&MaxAmount=&MinimalDate=&MaximalDate=&ContributionMode=&ContributionSearchType=All"
            f"&PageSize=10&PageIndex=0&ReportId="
        )

        headers = {
            "Referer": KY_CONTRIBUTIONS_SEARCH_URL,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
            ),
        }

        try:
            response = requests.get(search_url, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(
                f"Failed to load Contributions Search for {election_date}: {e}"
            )
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        export_link_tag = soup.find("a", title="Export report details")

        if isinstance(export_link_tag, Tag) and export_link_tag.get("href"):
            export_path = str(export_link_tag["href"])
            export_url = urljoin(KY_BASE_URL, export_path)

            base_dir = Path(KY_DATA_PATH_PREFIX).joinpath(name)
            base_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{name}_{election_date.replace('/', '-')}.csv"
            file_path = os.path.join(base_dir, file_name)

            stream_download_file_to_path(
                request_url=export_url,
                file_save_path=file_path,
                headers=headers,
            )

            logger.info(f"Saved CSV for {election_date} to {file_path}")


@dg.asset
def ky_fetch_expenditures_data():
    """
    Fetches candidate data for each election date in Kentucky.

    This function iterates over the election dates, constructs a search URL for
    each date, and sends requests to retrieve the candidate data. If the data is
    available, it downloads the CSV file containing the candidate data and saves
    it to a local directory for further processing.

    Steps:
    1. Retrieve election dates.
    2. Construct the search URL for each election date.
    3. Fetch the search results page and locate the export link.
    4. Download the corresponding CSV file.
    5. Save the CSV to the appropriate directory.

    The function logs key events including successes, failures, and missing
        export links.

    Args:
        None

    Returns:
        None
    """
    election_dates = fetch_ky_election_dates()
    logger = dg.get_dagster_logger(name="ky_fetch_expenditures_data")
    name = "expenditures"

    for election_date in election_dates:
        logger.info(f"Processing election date: {election_date}")
        encoded_date = quote_plus(election_date)

        search_url = (
            f"{KY_EXPENDITURES_SEARCH_URL}"
            f"?FirstName=&LastName=&OrganizationName="
            f"&FromCandidateFirstName=&FromCandidateLastName=&FromOrganizationName="
            f"&ElectionDate={encoded_date}"
            f"&ElectionType=&MinAmount=&MaxAmount="
            f"&MinimalDate=&MaximalDate=&PageSize=0&PageIndex=0&ReportId="
        )

        headers = {
            "Referer": KY_EXPENDITURES_SEARCH_URL,
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
            ),
        }

        try:
            response = requests.get(search_url, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to load expendituresearch for {election_date}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        export_link_tag = soup.find("a", title="Export search results")

        if isinstance(export_link_tag, Tag) and export_link_tag.get("href"):
            export_path = str(export_link_tag["href"])
            export_url = urljoin(KY_BASE_URL, export_path)

            base_dir = Path(KY_DATA_PATH_PREFIX).joinpath(name)
            base_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{name}_{election_date.replace('/', '-')}.csv"
            file_path = os.path.join(base_dir, file_name)

            stream_download_file_to_path(
                request_url=export_url,
                file_save_path=file_path,
                headers=headers,
            )

            logger.info(f"Saved CSV for {election_date} to {file_path}")


@dg.asset(deps=[ky_fetch_candidates_data])
def ky_insert_candidates_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Kentucky candidates CSVs into the PostgreSQL landing table.
    - Scans downloaded `candidates_*.csv` files inside the
        `candidates/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ky_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ky_candidates_landing",
        table_columns_name=[
            "last_name",
            "first_name",
            "office_sought",
            "is_active_label",
            "location",
            "election_date",
            "election_type",
            "exemption_status",
            "total_receipts",
            "total_disburse",
        ],
        data_validation_callback=lambda row: len(row) == 10,
        category="candidates",
    )


@dg.asset(deps=[ky_fetch_organizations_data])
def ky_insert_organizations_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Kentucky organizations CSVs into the PostgreSQL landing table.
    - Scans downloaded `organizations_*.csv` files inside the
        `organizations/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ky_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ky_organizations_landing",
        table_columns_name=[
            "organization_name",
            "organization_type",
            "candidate_first_name",
            "candidate_last_name",
            "political_issue_amendment_or_question",
            "political_view",
            "office_sought",
            "location",
            "election_date",
            "election_type",
            "chair_name",
            "chair_address1",
            "chair_address2",
            "chair_city",
            "chair_state",
            "chair_phone",
            "treasurer_name",
            "treasurer_address1",
            "treasurer_address2",
            "treasurer_city",
            "treasurer_state",
            "treasurer_phone",
            "contact_name",
            "contact_address1",
            "contact_address2",
            "contact_city",
            "contact_state",
            "contact_phone",
            "is_active_label",
        ],
        data_validation_callback=lambda row: len(row) == 29,
        category="organizations",
    )


@dg.asset(deps=[ky_fetch_contributions_data])
def ky_insert_contributions_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Kentucky contributions CSVs into the PostgreSQL landing table.
    - Scans downloaded `contributions_*.csv` files inside the
        `contributions/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ky_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ky_contributions_landing",
        table_columns_name=[
            "to_organization",
            "from_organization_name",
            "contributor_last_name",
            "contributor_first_name",
            "recipient_last_name",
            "recipient_first_name",
            "office_sought",
            "location",
            "election_date",
            "election_type",
            "exemption_status",
            "other_text",
            "address1",
            "address2",
            "city",
            "state",
            "zip",
            "amount",
            "contribution_type",
            "contribution_mode",
            "occupation",
            "other_occupation",
            "employer",
            "spouse_prefix",
            "spouse_last_name",
            "spouse_first_name",
            "spouse_middle_initial",
            "spouse_suffix",
            "spouse_occupation",
            "spouse_employer",
            "number_of_contributors",
            "inkind_description",
            "receipt_date",
            "statement_type",
        ],
        data_validation_callback=lambda row: len(row) == 34,
        category="contributions",
    )


@dg.asset(deps=[ky_fetch_expenditures_data])
def ky_insert_expenditures_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """
    Loads Kentucky expenditures CSVs into the PostgreSQL landing table.
    - Scans downloaded `expenditures_*.csv` files inside the
        `expenditures/` directory.
    - Validates each row (default: accepts all).
    - Truncates the target table before inserting.
    - Returns row count metadata.
    """
    return ky_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="ky_expenditures_landing",
        table_columns_name=[
            "recipient_last_name",
            "recipient_first_name",
            "organization_name",
            "purpose",
            "occupation",
            "disbursement_code",
            "disbursement_amount",
            "disbursement_date",
            "from_candidate_first_name",
            "from_candidate_last_name",
            "from_organization_name",
            "statement_type",
            "office_sought",
            "election_date",
            "election_type",
            "is_independent_expenditure",
        ],
        data_validation_callback=lambda row: len(row) == 16,
        category="expenditures",
    )

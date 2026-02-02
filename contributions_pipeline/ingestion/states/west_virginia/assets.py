import csv
import datetime
import glob
import os
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import requests
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

WV_DATA_PATH_PREFIX = "./states/west_virginia"
WV_BASE_URL = "https://cfrs.wvsos.gov/CFIS_APIService/api"
WV_COMMITTEES_SEARCH_URL = f"{WV_BASE_URL}/Organization/SearchCommittees"
WV_COMMITTEE_DETAILS_URL = f"{WV_BASE_URL}/Organization/GetCommitteeInformation"
WV_CALENDAR_YEARS_URL = f"{WV_BASE_URL}/CommitteeRegistration/GetCalendarYear"
WV_CANDIDATES_SEARCH_URL = f"{WV_BASE_URL}/Organization/SearchCandidates"
WV_CANDIDATE_DETAILS_URL = f"{WV_BASE_URL}/Organization/GetCandidatesInformation"
WV_ELECTION_YEARS_URL = f"{WV_BASE_URL}/CommitteeRegistration/GetElectionYear"

TRANSACTION_TYPES = {
    "CON": "contributions",
    "EXP": "expenditures",
}

# Headers for API requests
WV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://cfrs.wvsos.gov",
    "Referer": "https://cfrs.wvsos.gov/index.html",
}

# Define committee columns for CSV and PostgreSQL
WV_COMMITTEE_COLUMNS = [
    "committee_name",
    "committee_type",
    "committee_address",
    "phone",
    "election_year",
    "status",
    "total_contributions",
    "total_expenditures",
    "pac_type",
    "party",
    "id_number",
    "date_registered",
    "treasurer",
    "office_id",
    "election_id",
    "district_id",
    "election_cycle_id",
    "registration_id",
    "row_number",
    "total_rows",
    "committee_type_code",
    "district",
    "town",
]

# Define candidate columns for CSV and PostgreSQL
WV_CANDIDATE_COLUMNS = [
    "CandidateName",
    "OfficeName",
    "ElectionYear",
    "Party",
    "District",
    "Jurisdiction",
    "FinanceType",
    "Status",
    "Incumbent",
    "IDNumber",
    "TreasurerName",
    "CandidateAddress",
    "RegistrationDate",
    "PublicPhoneNumber",
    "PoliticalPartyCommitteeName",
    "CandidateStatus",
    "OfficerHolderStatus",
    "ElectionName",
    "NumberofCandidates",
    "OfficeId",
    "ElectionId",
    "DistrictId",
    "ElectionCycleId",
    "OfficeType",
    "JurisdictionId",
    "RegistrationId",
    "TotalContributions",
    "TotalExpenditures",
    "FinanceStatus",
    "RowNumber",
    "TotalRows",
    "UnregisteredCandidate",
    "ChairPesonName",
    "CommitteeSubtypeCode",
    "CashOnHand",
]

# Define contribution columns based on the actual CSV structure
WV_CONTRIBUTION_COLUMNS = [
    "org_id",
    "receipt_amount",
    "receipt_date",
    "last_name",
    "first_name",
    "middle_name",
    "suffix",
    "address1",
    "address2",
    "city",
    "state",
    "zip",
    "description",
    "receipt_id",
    "filed_date",
    "receipt_source_type",
    "amended",
    "receipt_type",
    "committee_type",
    "committee_name",
    "candidate_name",
    "employer",
    "occupation",
    "occupation_comment",
    "forgiven_loan",
    "fundraiser_event_date",
    "fundraiser_event_type",
    "fundraiser_event_place",
    "report_name",
    "contribution_type",
]

# Define expenditure columns based on the actual CSV structure
WV_EXPENDITURE_COLUMNS = [
    "org_id",
    "expenditure_amount",
    "expenditure_date",
    "last_name",
    "first_name",
    "middle_name",
    "suffix",
    "address1",
    "address2",
    "city",
    "state",
    "zip",
    "explanation",
    "expenditure_id",
    "filed_date",
    "purpose",
    "amended",
    "expenditure_type",
    "committee_type",
    "committee_name",
    "candidate_name",
    "fundraiser_event_date",
    "fundraiser_event_type",
    "fundraiser_event_place",
    "support_or_oppose",
    "candidate",
    "report_name",
    "expenditure_category",
]

# ===== Partition Definition =====
# Unified partition definition starting from 2003 to cover all data types
wv_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=datetime.datetime(2003, 1, 1),
    end_offset=1,
)


# ===== Common Functions =====
def wv_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
    data_files: list[str] | None = None,
    truncate_query: sql.Composed | None = None,
) -> dg.MaterializeResult:
    """
    Insert raw CSV data from previously downloaded files into the specified
    PostgreSQL landing table.

    Args:
        postgres_pool: Postgres connection pool
        table_name: Name of the landing table
        table_columns_name: List of column names
        data_validation_callback: Function to validate data rows
        category: The category of data (committees, contributions, etc.)
        data_files: Optional list of specific file paths to process.
                   If None, automatically discovers CSV files in the category directory
                   using pattern "*_{category}.csv"
        truncate_query: Optional custom truncate query.
                       If None, uses the default truncate query that
                       clears the entire table

    Returns:
        MaterializeResult with metadata about the operation
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    if data_files is None:
        # Use automatic file discovery for non-contributions
        base_path = Path(WV_DATA_PATH_PREFIX)
        data_files = []

        glob_path = base_path / category
        if glob_path.exists():
            # Look for CSV files only
            csv_files = glob.glob(str(glob_path / f"*_{category}.csv"))
            data_files.extend(csv_files)
    else:
        # Use the provided data_files list
        data_files = [str(f) for f in data_files]

    logger.info(f"Category: {category} | Found {len(data_files)} files.")

    if truncate_query is None:
        truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Executing truncate for table {table_name}.")
        pg_cursor.execute(query=truncate_query)

        try:
            for data_file in data_files:
                if not os.path.exists(data_file):
                    logger.warning(f"File {data_file} does not exist, skipping...")
                    continue

                logger.info(f"Inserting from: {data_file}")

                # Process CSV files
                logger.info(f"Processing CSV file: {data_file}")

                data_type_lines_generator = safe_readline_csv_like_file(
                    data_file, encoding="utf-8"
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=","
                )

                # Skip header
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


# ===== Fetch Assets =====
@dg.asset(
    partitions_def=wv_yearly_partition,
)
def wv_fetch_contributions_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches and downloads raw campaign finance data for West Virginia from the official
    state website (https://cfrs.wvsos.gov/index.html#/dataDownload) for a specific year.

    This asset is partitioned by year, fetching data for each year starting from 2018.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2018 when contributions data became available
    if year < 2018:
        logger.info(
            f"Contributions data not available for year {year} (before 2018), skipping"
        )
        return

    logger.info(f"Downloading contributions data for {year}")

    url = (
        f"{WV_BASE_URL}/DataDownload/GetCSVDownloadReport?year={year}"
        f"&transactionType=CON"
        f"&reportFormat=csv"
        f"&fileName=CON_{year}.csv"
    )

    logger.info(f"Downloading contributions data for {year} with url {url}")
    category_dir = Path(WV_DATA_PATH_PREFIX, "contributions")
    category_dir.mkdir(parents=True, exist_ok=True)
    file_path = category_dir / f"{year}_contributions.csv"

    stream_download_file_to_path(request_url=url, file_save_path=file_path)


@dg.asset(
    partitions_def=wv_yearly_partition,
)
def wv_fetch_expenditures_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches and downloads raw expenditure data for West Virginia from the official
    state website for a specific year.

    This asset is partitioned by year, fetching data for each year starting from 2018.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2018 when expenditures data became available
    if year < 2018:
        logger.info(
            f"Expenditures data not available for year {year} (before 2018), skipping"
        )
        return

    logger.info(f"Downloading expenditures data for {year}")

    url = (
        f"{WV_BASE_URL}/DataDownload/GetCSVDownloadReport?year={year}"
        f"&transactionType=EXP"
        f"&reportFormat=csv"
        f"&fileName=EXP_{year}.csv"
    )

    logger.info(f"Downloading expenditures data for {year} with url {url}")
    category_dir = Path(WV_DATA_PATH_PREFIX, "expenditures")
    category_dir.mkdir(parents=True, exist_ok=True)
    file_path = category_dir / f"{year}_expenditures.csv"

    stream_download_file_to_path(request_url=url, file_save_path=file_path)


@dg.asset(
    partitions_def=wv_yearly_partition,
)
def wv_fetch_committees_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches committee data from West Virginia's campaign finance system for a specific
    year. Downloads committee details for each committee found in the search results.
    This asset is partitioned by year, fetching data for each year starting from 2003.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2003 when committees data became available
    if year < 2003:
        logger.info(
            f"Committees data not available for year {year} (before 2003), skipping"
        )
        return

    try:
        # Initialize session with SSL verification disabled
        session = requests.Session()
        session.headers.update(WV_HEADERS)
        session.verify = False

        logger.info(f"Processing year {year}")
        page_number = 1
        total_rows = 0
        year_committees = []

        while True:
            # Prepare search payload
            search_payload = {
                "electionYear": year,
                "party": "",
                "committeeType": "",
                "transactionType": "",
                "transactionAmount": None,
                "ballotQuestions": None,
                "stance": "",
                "pacType": "",
                "status": "",
                "BallotQuestionOnly": None,
                "pageNumber": page_number,
                "pageSize": 50,
                "sortDir": "asc",
                "sortedBy": "",
            }

            # Make search request
            logger.info(f"Fetching page {page_number} for year {year}...")
            search_response = session.post(
                WV_COMMITTEES_SEARCH_URL, json=search_payload
            )
            search_response.raise_for_status()

            # Parse search results
            response_data = search_response.json()
            committees = response_data

            # Get total rows from first page
            if total_rows == 0 and committees:
                total_rows = int(committees[0].get("TotalRows", 0))
                logger.info(f"Total committees for year {year}: {total_rows}")

            if not committees:
                break

            # Fetch details for each committee
            for committee in committees:
                member_id = committee.get("IdNumber")
                if not member_id:
                    continue

                logger.info(f"Fetching details for committee {member_id}")

                # Get committee details
                details_response = session.get(
                    WV_COMMITTEE_DETAILS_URL, params={"memberId": member_id}
                )

                committee_details = details_response.json()
                if committee_details:
                    year_committees.extend(
                        {**c, "election_year": str(year)} for c in committee_details
                    )

            # Check if we've processed all pages
            if page_number * 50 >= total_rows:
                break

            page_number += 1

        # Save year's committees data as CSV
        name_dir = Path(WV_DATA_PATH_PREFIX) / "committees"
        name_dir.mkdir(parents=True, exist_ok=True)

        csv_file = name_dir / f"{year}_committees.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=WV_COMMITTEE_COLUMNS)
            writer.writeheader()

            # Convert keys to lowercase and map to column names
            for committee in year_committees:
                row = {}
                for key, value in committee.items():
                    snake_key = "".join(
                        ["_" + c.lower() if c.isupper() else c for c in key]
                    ).lstrip("_")
                    if snake_key in WV_COMMITTEE_COLUMNS:
                        row[snake_key] = value
                writer.writerow(row)

        logger.info(
            f"Saved {len(year_committees)} committee records for year {year} "
            f"to {csv_file}"
        )

    except Exception as e:
        logger.error(f"Error fetching West Virginia committees for year {year}: {e!s}")
        raise


@dg.asset(
    partitions_def=wv_yearly_partition,
)
def wv_fetch_candidates_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches candidate data from West Virginia's campaign finance system for a specific
    year. Downloads candidate details for each candidate found in the search results.
    This asset is partitioned by year, fetching data for each year starting from 2004.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2004 when candidates data became available
    if year < 2004:
        logger.info(
            f"Candidates data not available for year {year} (before 2004), skipping"
        )
        return

    try:
        # Initialize session with SSL verification disabled
        session = requests.Session()
        session.headers.update(WV_HEADERS)
        session.verify = False

        logger.info(f"Processing election year {year}")
        page_number = 1
        total_rows = 0
        year_candidates = []

        while True:
            # Prepare search payload
            search_payload = {
                "ElectionYear": year,
                "Party": None,
                "OfficeSought": None,
                "JurisdictionType": None,
                "Jurisdiction": None,
                "DistrictId": None,
                "FinanceType": None,
                "IsPreCandidateInclude": True,
                "Status": "",
                "TransactionAmount": None,
                "TransactionType": None,
                "pageNumber": page_number,
                "pageSize": 50,
                "sortDir": "ASC",
                "sortedBy": "CandidateName",
            }

            # Make search request
            logger.info(f"Fetching page {page_number} for year {year}...")
            search_response = session.post(
                WV_CANDIDATES_SEARCH_URL, json=search_payload
            )
            search_response.raise_for_status()

            # Parse search results
            response_data = search_response.json()
            candidates = response_data

            # Get total rows from first page
            if total_rows == 0 and candidates:
                total_rows = int(candidates[0].get("TotalRows", 0))
                logger.info(f"Total candidates for year {year}: {total_rows}")

            if not candidates:
                break

            # Fetch details for each candidate
            for candidate in candidates:
                member_id = candidate.get("IDNumber")
                if not member_id:
                    continue

                logger.info(f"Fetching details for candidate {member_id}")

                # Get candidate details
                details_response = session.get(
                    WV_CANDIDATE_DETAILS_URL, params={"memberId": member_id}
                )

                candidate_details = details_response.json()
                if candidate_details:
                    year_candidates.extend(candidate_details)

            # Check if we've processed all pages
            if page_number * 50 >= total_rows:
                break

            page_number += 1

        # Save year's candidates data as CSV
        name_dir = Path(WV_DATA_PATH_PREFIX) / "candidates"
        name_dir.mkdir(parents=True, exist_ok=True)

        csv_file = name_dir / f"{year}_candidates.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=WV_CANDIDATE_COLUMNS)
            writer.writeheader()

            # Map candidate data directly to columns
            for candidate in year_candidates:
                row = {}
                for key, value in candidate.items():
                    if key in WV_CANDIDATE_COLUMNS:
                        row[key] = value
                writer.writerow(row)

        logger.info(
            f"Saved {len(year_candidates)} candidate records for year {year} "
            f"to {csv_file}"
        )

    except Exception as e:
        logger.error(f"Error fetching West Virginia candidates for year {year}: {e!s}")
        raise


# ===== Insert Assets =====
@dg.asset(
    deps=[wv_fetch_committees_raw_data],
    partitions_def=wv_yearly_partition,
)
def wv_insert_committees_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert committee data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2003 when committees data became available
    if year < 2003:
        logger.info(
            f"Committees data not available for year {year} (before 2003), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_committees_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all committee files for the specific year
    data_dir = Path(WV_DATA_PATH_PREFIX) / "committees"
    committee_files = list(data_dir.glob(f"{year}_committees.csv"))

    if not committee_files:
        logger.info(f"No committee files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_committees_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "wv_committees_landing"
    table_name_identifier = sql.Identifier(table_name)

    # Create a truncate query that only deletes data for the specific year
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE election_year = {year}"
    ).format(table_name=table_name_identifier, year=sql.Literal(str(year)))

    return wv_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=WV_COMMITTEE_COLUMNS,
        data_validation_callback=lambda row: len(row) == len(WV_COMMITTEE_COLUMNS),
        category="committees",
        data_files=[str(f) for f in committee_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[wv_fetch_candidates_raw_data],
    partitions_def=wv_yearly_partition,
)
def wv_insert_candidates_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert candidate data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2004 when candidates data became available
    if year < 2004:
        logger.info(
            f"Candidates data not available for year {year} (before 2004), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_candidates_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all candidate files for the specific year
    data_dir = Path(WV_DATA_PATH_PREFIX) / "candidates"
    candidate_files = list(data_dir.glob(f"{year}_candidates.csv"))

    if not candidate_files:
        logger.info(f"No candidate files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_candidates_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "wv_candidates_landing"
    table_name_identifier = sql.Identifier(table_name)

    # Create a truncate query that only deletes data for the specific year
    truncate_query = sql.SQL(
        'DELETE FROM {table_name} WHERE "ElectionYear" = {year}'
    ).format(table_name=table_name_identifier, year=sql.Literal(str(year)))

    return wv_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=WV_CANDIDATE_COLUMNS,
        data_validation_callback=lambda row: len(row) == len(WV_CANDIDATE_COLUMNS),
        category="candidates",
        data_files=[str(f) for f in candidate_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[wv_fetch_contributions_raw_data],
    partitions_def=wv_yearly_partition,
)
def wv_insert_raw_contributions_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert contributions data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2018 when contributions data became available
    if year < 2018:
        logger.info(
            f"Contributions data not available for year {year} (before 2018), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_contributions_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all contribution CSV files for the specific year
    data_dir = Path(WV_DATA_PATH_PREFIX) / "contributions"
    contribution_files = list(data_dir.glob(f"{year}_contributions.csv"))

    if not contribution_files:
        logger.info(f"No contribution files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_contributions_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "wv_contributions_landing"
    table_name_identifier = sql.Identifier(table_name)

    # Create a truncate query that only deletes data for the specific year
    # Use filed_date instead of receipt_date since it represents when the report
    # was filed and is more reliable for year-based filtering. This addresses the issue
    # contributions from previous years (e.g., 2022 contributions filed in 2023 reports)
    # would not be properly cleaned up if we used receipt_date for filtering.
    # filed_date format: "M/D/YYYY H:M:S AM/PM" (stored as text)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE "
        "EXTRACT(YEAR FROM filed_date::timestamp) = {year}"
    ).format(table_name=table_name_identifier, year=sql.Literal(year))

    return wv_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=WV_CONTRIBUTION_COLUMNS,
        data_validation_callback=lambda row: len(row) == len(WV_CONTRIBUTION_COLUMNS),
        category="contributions",
        data_files=[str(f) for f in contribution_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[wv_fetch_expenditures_raw_data],
    partitions_def=wv_yearly_partition,
)
def wv_insert_raw_expenditures_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert expenditures data into the landing table for a specific election year.
    Deletes existing data for the election year before inserting new data.
    Uses election year (partition year) for data management and cleanup.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2018 when expenditures data became available
    if year < 2018:
        logger.info(
            f"Expenditures data not available for year {year} (before 2018), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_expenditures_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all expenditure CSV files for the specific year
    data_dir = Path(WV_DATA_PATH_PREFIX) / "expenditures"
    expenditure_files = list(data_dir.glob(f"{year}_expenditures.csv"))

    if not expenditure_files:
        logger.info(f"No expenditure files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "wv_expenditures_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "wv_expenditures_landing"
    table_name_identifier = sql.Identifier(table_name)

    # Create a truncate query that only deletes data for the specific year
    # Use filed_date instead of expenditure_date since it represents when the report
    # was filed
    # and is more reliable for year-based filtering. This addresses the issue where
    # expenditures from previous years (e.g., 2022 expenditures filed in 2023 reports)
    # would not be properly cleaned up if we used expenditure_date for filtering.
    # filed_date format: "M/D/YYYY H:M:S AM/PM" (stored as text)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE "
        "EXTRACT(YEAR FROM filed_date::timestamp) = {year}"
    ).format(table_name=table_name_identifier, year=sql.Literal(year))

    return wv_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=WV_EXPENDITURE_COLUMNS,
        data_validation_callback=lambda row: len(row) == len(WV_EXPENDITURE_COLUMNS),
        category="expenditures",
        data_files=[str(f) for f in expenditure_files],
        truncate_query=truncate_query,
    )

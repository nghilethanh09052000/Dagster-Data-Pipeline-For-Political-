import asyncio
import csv
import glob
import json
import os
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import aiohttp
import dagster as dg
import requests
from psycopg import sql
from psycopg_pool import ConnectionPool
from requests.exceptions import RequestException

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

MT_DATA_PATH_PREFIX = "./states/montana"
MT_BASE_URL = "https://camptrackext.mt.gov"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Host": "camptrackext.mt.gov",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": ('"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'),
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
}

# Constants for candidates data fetching
MT_CANDIDATES_NAME = "candidates"
MT_CANDIDATES_SEARCH_URL = "https://cers-ext.mt.gov/CampaignTracker/public/search"
MT_CANDIDATES_POST_URL = (
    "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/searchCandidates"
)
MT_CANDIDATES_BASE_URL = (
    "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/listCandidateResults"
)
MT_CANDIDATES_PAGE_SIZE = 100
MT_CANDIDATES_HEADERS = [
    "candidate_id",
    "ent_id",
    "candidate_name",
    "candidate_address",
    "office_code",
    "office_title",
    "candidate_type_code",
    "candidate_type_descr",
    "res_county_code",
    "res_county_descr",
    "party_code",
    "party_descr",
    "own_treasurer_ind",
    "election_year",
    "closed_ind",
    "candidate_status_code",
    "candidate_status_descr",
    "c3_filed_ind",
    "amended_date",
    "created_date",
    "first_name",
    "last_name",
    "middle_initial",
    "entity_type_descr",
    "email",
    "mailing_address",
    "home_phone",
    "work_phone",
]
# Constants for committees data fetching
MT_COMMITTEES_SEARCH_URL = "https://cers-ext.mt.gov/CampaignTracker/public/search"
MT_COMMITTEES_POST_URL = (
    "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/searchCommittees"
)
MT_COMMITTEES_BASE_URL = (
    "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/listCommitteeResults"
)
MT_COMMITTEES_HEADERS = [
    "committee_id",
    "ent_id",
    "committee_name",
    "committee_address",
    "committee_type_code",
    "committee_type_descr",
    "election_year",
    "over_500_ind",
    "comments",
    "committee_subtype_code",
    "committee_subtype_descr",
    "county_code",
    "county_descr",
    "incorporated",
    "committee_status_code",
    "committee_status_descr",
    "c2_is_filed",
    "created_date",
    "amended_date",
    # Entity details
    "entity_type_code",
    "entity_type_descr",
    "entity_name",
    "email",
    # Address details
    "mailing_address",
    "city",
    "state",
    "zip5",
    "zip4",
    "country_code",
    "country_descr",
    # Phone details
    "phone_number",
    "phone_type_code",
    "phone_type_descr",
    "phone_formatted",
]


# Constants for contribution details landing table
MT_CONTRIBUTION_COMMITTEES_DETAILS_HEADERS = [
    "committee_id",
    "committee_name",
    "committee_type",
    "election_year",
    "transaction_id",
    "transaction_date",
    "transaction_amount",
    "total_to_date",
    "contributor_name",
    "contributor_address",
    "contributor_city",
    "contributor_state",
    "transaction_type",
    "schedule",
    "amount_type",
    "description",
    "purpose",
    "occupation",
    "employer",
]

# Constants for candidate contribution details landing table
MT_CONTRIBUTION_CANDIDATES_DETAILS_HEADERS = [
    "candidate_id",
    "candidate_name",
    "office_title",
    "party_descr",
    "election_year",
    "transaction_id",
    "transaction_date",
    "transaction_amount",
    "total_to_date",
    "contributor_name",
    "contributor_address",
    "contributor_city",
    "contributor_state",
    "transaction_type",
    "schedule",
    "amount_type",
    "description",
    "purpose",
    "occupation",
    "employer",
]


def mt_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
    partition_key: str | None = None,
    truncate_query: sql.Composed | None = None,
) -> dg.MaterializeResult:
    """Insert raw CSV data from previously downloaded files into the
        specified PostgreSQL landing table.

    Args:
        postgres_pool (ConnectionPool): A connection pool for PostgreSQL,
                                    typically injected from a Dagster resource.
        table_name (str): Name of the landing table to truncate and insert.
        table_columns_name (List[str]): List of column names in the correct order
            for insert.
        data_validation_callback (Callable): A function that takes a CSV row and returns
                                    a boolean indicating if the row should be inserted.
        category (str): The subdirectory under each year (e.g., 'candidates'),
                        used to locate CSV files inside

    Returns:
        MaterializeResult: A Dagster metadata result object containing
                            the table name and the number of rows inserted.
    """
    logger = dg.get_dagster_logger(name=f"{category}_insert")

    base_path = Path(MT_DATA_PATH_PREFIX)
    data_files = []

    glob_path = base_path / category

    if partition_key is not None:
        glob_path = glob_path / partition_key

    logger.info(f"Glob Path: {glob_path}")
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


def get_election_years():
    """Get a list of election years from 1990 to current year.

    Returns:
        List[int]: A list of election years in descending order.
    """
    logger = dg.get_dagster_logger("get_election_years")

    current_year = datetime.now().year
    years = list(range(1990, current_year + 1))

    logger.info(f"Generated {len(years)} election years from 1990 to {current_year}")
    return sorted(years, reverse=True)  # Return years in descending order


@dg.asset(pool="mt_api")
def mt_fetch_candidates_data():
    """Fetch candidate information from the Montana COPP Campaign Track portal.

    This function:
    1. Creates a session and sets up headers
    2. Gets election years
    3. For each year:
       - Makes search request
       - Processes paginated results
       - Saves data to CSV files
    4. Returns metadata about the fetched data

    Returns:
        MaterializeResult: Contains metadata about the fetched data including
                          number of candidates and output file paths.
    """
    logger = dg.get_dagster_logger("mt_fetch_candidates_data")

    # CSV headers for candidates data

    name = "candidates"
    # Create directory for storing candidate data
    base_dir = Path(MT_DATA_PATH_PREFIX).joinpath(name)
    base_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info("Fetching candidates data...")

    # Get election years
    election_years = get_election_years()
    if not election_years:
        logger.error("No election years found")
        return dg.MaterializeResult(metadata={"dagster/num_candidates": 0})

    total_candidates = 0
    output_files = []

    for year in election_years:
        logger.info(f"Fetching candidates for election year {year}")

        # Prepare the POST payload
        payload = {
            "lastName": "",
            "firstName": "",
            "middleInitial": "",
            "electionYear": year,
            "candidateTypeCode": "",
            "officeCode": "",
            "countyCode": "",
            "partyCode": "",
        }

        try:
            # First get the search page to set up the session
            response = session.get(MT_CANDIDATES_SEARCH_URL)
            response.raise_for_status()

            # Then make the POST request
            response = session.post(MT_CANDIDATES_POST_URL, data=payload)
            response.raise_for_status()

            # Now fetch the candidate results
            base_url = MT_CANDIDATES_BASE_URL
            page_size = 100
            current_page = 0
            total_records = 0
            year_candidates = []

            while True:
                params = {
                    "sEcho": "2",
                    "iColumns": "9",
                    "sColumns": "",
                    "iDisplayStart": current_page * page_size,
                    "iDisplayLength": page_size,
                    "mDataProp_0": "checked",
                    "mDataProp_1": "candidateName",
                    "mDataProp_2": "electionYear",
                    "mDataProp_3": "candidateStatusDescr",
                    "mDataProp_4": "c3FiledInd",
                    "mDataProp_5": "candidateAddress",
                    "mDataProp_6": "candidateTypeDescr",
                    "mDataProp_7": "officeTitle",
                    "mDataProp_8": "resCountyDescr",
                    "sSearch": "",
                    "bRegex": "false",
                    "iSortCol_0": "0",
                    "sSortDir_0": "asc",
                    "iSortingCols": "1",
                    "_": str(int(time.time() * 1000)),
                }

                response = session.get(base_url, params=params)
                response.raise_for_status()

                data = response.json()

                if current_page == 0:
                    total_records = data.get("iTotalRecords", 0)
                    logger.info(f"Total candidates for year {year}: {total_records}")

                candidates = data.get("aaData", [])
                if not candidates:
                    break

                # Process each candidate's data
                for candidate in candidates:
                    # Safely get nested dictionaries with default empty dicts
                    person_dto = candidate.get("personDTO") or {}
                    mail_address_dto = (
                        (person_dto.get("mailAddressDTO") or {}) if person_dto else {}
                    )
                    home_phone_dto = (
                        (person_dto.get("homePhoneDTO") or {}) if person_dto else {}
                    )
                    work_phone_dto = (
                        (person_dto.get("workPhoneDTO") or {}) if person_dto else {}
                    )

                    processed_candidate = {
                        "candidate_id": candidate.get("candidateId"),
                        "ent_id": candidate.get("entId"),
                        "candidate_name": candidate.get("candidateName"),
                        "candidate_address": candidate.get("candidateAddress"),
                        "office_code": candidate.get("officeCode"),
                        "office_title": candidate.get("officeTitle"),
                        "candidate_type_code": candidate.get("candidateTypeCode"),
                        "candidate_type_descr": candidate.get("candidateTypeDescr"),
                        "res_county_code": candidate.get("resCountyCode"),
                        "res_county_descr": candidate.get("resCountyDescr"),
                        "party_code": candidate.get("partyCode"),
                        "party_descr": candidate.get("partyDescr"),
                        "own_treasurer_ind": candidate.get("ownTreasurerInd"),
                        "election_year": candidate.get("electionYear"),
                        "closed_ind": candidate.get("closedInd"),
                        "candidate_status_code": candidate.get("candidateStatusCode"),
                        "candidate_status_descr": candidate.get("candidateStatusDescr"),
                        "c3_filed_ind": candidate.get("c3FiledInd"),
                        "amended_date": candidate.get("amendedDate"),
                        "created_date": candidate.get("createdDate"),
                        # Person details
                        "first_name": person_dto.get("firstName"),
                        "last_name": person_dto.get("lastName"),
                        "middle_initial": person_dto.get("middleInitial"),
                        "entity_type_descr": person_dto.get("entityTypeDescr"),
                        # Contact information
                        "email": person_dto.get("emailAddress"),
                        "mailing_address": mail_address_dto.get("addrCityStateZip"),
                        "home_phone": home_phone_dto.get("phoneNumFormatted"),
                        "work_phone": work_phone_dto.get("phoneNumFormatted"),
                    }
                    year_candidates.append(processed_candidate)

                logger.info(
                    f"Fetched page {current_page + 1}\
                            for year {year} with {len(candidates)} candidates"
                )

                if len(year_candidates) >= total_records:
                    break

                current_page += 1

            # Save year's candidates to CSV
            if year_candidates:
                output_file = base_dir / f"{year}_candidates.csv"

                with open(output_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=MT_CANDIDATES_HEADERS)
                    writer.writeheader()
                    writer.writerows(year_candidates)

                logger.info(
                    f"Saved {len(year_candidates)} candidates\
                    for year {year} to {output_file}"
                )
                total_candidates += len(year_candidates)
                output_files.append(str(output_file))

        except RequestException as e:
            logger.error(f"Error fetching candidates for year {year}: {e}")
            continue
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response for year {year}: {e}")
            continue

    if total_candidates > 0:
        return dg.MaterializeResult(
            metadata={
                "dagster/num_candidates": total_candidates,
                "dagster/output_files": output_files,
            }
        )
    else:
        logger.warning("No candidate data was fetched")
        return dg.MaterializeResult(metadata={"dagster/num_candidates": 0})


@dg.asset(pool="mt_api")
def mt_fetch_committees_data():
    """Fetch committee information from the Montana COPP Campaign Track portal.

    This function:
    1. Creates a session and sets up headers
    2. Gets election years
    3. For each year:
       - Makes search request
       - Processes paginated results
       - Saves data to CSV files
    4. Returns metadata about the fetched data

    Returns:
        MaterializeResult: Contains metadata about the fetched data including
                          number of committees and output file paths.
    """
    logger = dg.get_dagster_logger("mt_fetch_committees_data")

    name = "committees"
    # Create directory for storing committee data
    base_dir = Path(MT_DATA_PATH_PREFIX).joinpath(name)
    base_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info("Fetching committees data...")

    # Get election years
    election_years = get_election_years()
    if not election_years:
        logger.error("No election years found")
        return dg.MaterializeResult(metadata={"dagster/num_committees": 0})

    total_committees = 0
    output_files = []

    for year in election_years:
        logger.info(f"Fetching committees for election year {year}")

        # Prepare the POST payload
        payload = {"committeeName": "", "electionYear": year, "committeeTypeCode": ""}

        try:
            # First get the search page to set up the session
            response = session.get(MT_COMMITTEES_SEARCH_URL)
            response.raise_for_status()

            # Then make the POST request
            response = session.post(MT_COMMITTEES_POST_URL, data=payload)
            response.raise_for_status()

            # Now fetch the committee results
            base_url = MT_COMMITTEES_BASE_URL
            page_size = 100
            current_page = 0
            total_records = 0
            year_committees = []

            while True:
                params = {
                    "sEcho": "2",
                    "iColumns": "6",
                    "sColumns": "",
                    "iDisplayStart": current_page * page_size,
                    "iDisplayLength": page_size,
                    "mDataProp_0": "checked",
                    "mDataProp_1": "committeeName",
                    "mDataProp_2": "electionYear",
                    "mDataProp_3": "committeeStatusDescr",
                    "mDataProp_4": "committeeAddress",
                    "mDataProp_5": "committeeTypeDescr",
                    "sSearch": "",
                    "bRegex": "false",
                    "iSortCol_0": "0",
                    "sSortDir_0": "asc",
                    "iSortingCols": "1",
                    "_": str(int(time.time() * 1000)),
                }

                response = session.get(base_url, params=params)
                response.raise_for_status()

                data = response.json()

                if current_page == 0:
                    total_records = data.get("iTotalRecords", 0)
                    logger.info(f"Total committees for year {year}: {total_records}")

                committees = data.get("aaData", [])
                if not committees:
                    break

                # Process each committee's data
                for committee in committees:
                    # Safely get nested dictionaries with default empty dicts
                    entity_dto = committee.get("entityDTO") or {}
                    mail_address_dto = entity_dto.get("mailAddressDTO") or {}
                    work_phone_dto = entity_dto.get("workPhoneDTO") or {}

                    processed_committee = {
                        "committee_id": committee.get("committeeId"),
                        "ent_id": committee.get("entId"),
                        "committee_name": committee.get("committeeName"),
                        "committee_address": committee.get("committeeAddress"),
                        "committee_type_code": committee.get("committeeTypeCode"),
                        "committee_type_descr": committee.get("committeeTypeDescr"),
                        "election_year": committee.get("electionYear"),
                        "over_500_ind": committee.get("over500Ind"),
                        "comments": committee.get("comments"),
                        "committee_subtype_code": committee.get("committeeSubtypeCode"),
                        "committee_subtype_descr": (
                            committee.get("committeeSubtypeDescr")
                        ),
                        "county_code": committee.get("countyCode"),
                        "county_descr": committee.get("countyDescr"),
                        "incorporated": committee.get("incorporated"),
                        "committee_status_code": committee.get("committeeStatusCode"),
                        "committee_status_descr": committee.get("committeeStatusDescr"),
                        "c2_is_filed": committee.get("c2IsFiled"),
                        "created_date": committee.get("createdDate"),
                        "amended_date": committee.get("amendedDate"),
                        # Entity details
                        "entity_type_code": entity_dto.get("entityTypeCode"),
                        "entity_type_descr": entity_dto.get("entityTypeDescr"),
                        "entity_name": entity_dto.get("entityName"),
                        "email": entity_dto.get("emailAddress"),
                        # Address details
                        "mailing_address": mail_address_dto.get("addrCityStateZip"),
                        "city": mail_address_dto.get("city"),
                        "state": mail_address_dto.get("state"),
                        "zip5": mail_address_dto.get("zip5"),
                        "zip4": mail_address_dto.get("zip4"),
                        "country_code": mail_address_dto.get("countryCode"),
                        "country_descr": mail_address_dto.get("countryDescr"),
                        # Phone details
                        "phone_number": work_phone_dto.get("phoneNum"),
                        "phone_type_code": work_phone_dto.get("phoneTypeCode"),
                        "phone_type_descr": work_phone_dto.get("phoneTypeDescr"),
                        "phone_formatted": work_phone_dto.get("phoneNumFormatted"),
                    }
                    year_committees.append(processed_committee)

                logger.info(
                    f"Fetched page {current_page + 1} for year {year}\
                    with {len(committees)} committees"
                )

                if len(year_committees) >= total_records:
                    break

                current_page += 1

            # Save year's committees to CSV
            if year_committees:
                output_file = base_dir / f"{year}_committees.csv"

                with open(output_file, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=MT_COMMITTEES_HEADERS)
                    writer.writeheader()
                    writer.writerows(year_committees)

                logger.info(
                    f"Saved {len(year_committees)} committees for year {year}\
                    to {output_file}"
                )
                total_committees += len(year_committees)
                output_files.append(str(output_file))

        except RequestException as e:
            logger.error(f"Error fetching committees for year {year}: {e}")
            continue
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response for year {year}: {e}")
            continue

    if total_committees > 0:
        return dg.MaterializeResult(
            metadata={
                "dagster/num_committees": total_committees,
                "dagster/output_files": output_files,
            }
        )
    else:
        logger.warning("No committee data was fetched")
        return dg.MaterializeResult(metadata={"dagster/num_committees": 0})


@dg.asset(deps=[mt_fetch_candidates_data])
def mt_inserting_candidates_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert candidate data into the landing table.

    Args:
        pg (ResourceParam[PostgresResource]): The PostgreSQL resource from Dagster.

    Returns:
        MaterializeResult: Contains metadata about the insertion process
    """
    return mt_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="mt_candidates_landing",
        table_columns_name=MT_CANDIDATES_HEADERS,
        data_validation_callback=(lambda row: len(row) == len(MT_CANDIDATES_HEADERS)),
        category="candidates",
    )


@dg.asset(deps=[mt_fetch_committees_data])
def mt_inserting_committees_reports_data(pg: dg.ResourceParam[PostgresResource]):
    """Insert committee data into the landing table.

    Args:
        pg (ResourceParam[PostgresResource]): The PostgreSQL resource from Dagster.

    Returns:
        MaterializeResult: Contains metadata about the insertion process
    """
    return mt_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name="mt_committees_landing",
        table_columns_name=MT_COMMITTEES_HEADERS,
        data_validation_callback=(lambda row: len(row) == len(MT_COMMITTEES_HEADERS)),
        category="committees",
    )


MT_FIRST_YEAR_DATA_AVAILABLE = datetime(year=2016, month=1, day=1)

mt_contributions_daily_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=MT_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(partitions_def=mt_contributions_daily_partition, pool="mt_api")
async def mt_fetch_contribution_committees_for_year(context: dg.AssetExecutionContext):
    """Fetch contribution details for committees in a specific year."""

    # As the partition key is just the string representation of the year,
    # we can do this cast safely
    year = int(context.partition_key)
    logger = dg.get_dagster_logger(f"mt_fetch_contribution_committees_for_year_{year}")

    # Create directory for storing committee data
    base_dir = (
        Path(MT_DATA_PATH_PREFIX) / "contributions_committees_details" / str(year)
    )
    base_dir.mkdir(parents=True, exist_ok=True)

    session = aiohttp.ClientSession()
    session.headers.update(HEADERS)

    logger.info(f"Fetching committee contributions for election year {year}")

    # Prepare the POST payload
    payload = {
        "financialSearchType": "CONTR",
        "contrSearchTypeCode": "COMMITTEE",
        "contrCanLastName": "",
        "contrCanFirstName": "",
        "contrCommitteeName": "",
        "contributorLastName": "",
        "contributorFirstName": "",
        "contrPartyCode": "",
        "contrCandidateTypeCode": "",
        "contrOfficeCode": "",
        "contrContributorTypeCode": "",
        "contrAmountRangeCode": "",
        "electionYear": year,
        "contrSearchFromDate": "",
        "contrSearchToDate": "",
    }

    total_committees = 0
    total_contributions = 0
    output_files = []

    try:
        # First get the search page to set up the session
        response = await session.get(
            "https://cers-ext.mt.gov/CampaignTracker/public/search"
        )
        response.raise_for_status()

        # Then make the POST request
        response = await session.post(
            "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/searchFinancials",
            data=payload,
        )
        response.raise_for_status()

        # Now fetch the committee results
        base_url = "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/listFinancialCommitteeResults"
        page_size = 100
        current_page = 0
        total_records = 0

        while True:
            params = {
                "sEcho": "2",
                "iColumns": "4",
                "sColumns": "",
                "iDisplayStart": current_page * page_size,
                "iDisplayLength": page_size,
                "mDataProp_0": "checked",
                "mDataProp_1": "committeeName",
                "mDataProp_2": "electionYear",
                "mDataProp_3": "committeeTypeDescr",
                "sSearch": "",
                "bRegex": "false",
                "iSortCol_0": "0",
                "sSortDir_0": "asc",
                "iSortingCols": "1",
                "_": str(int(time.time() * 1000)),
            }

            response = await session.get(base_url, params=params)
            response.raise_for_status()

            data = await response.json()

            if current_page == 0:
                total_records = data.get("iTotalRecords", 0)
                logger.info(f"Total committees for year {year}: {total_records}")

            committees = data.get("aaData", [])
            if not committees:
                break

            async def process_committee(committee, committee_id: str):
                try:
                    data = {"candidateId": "0", "committeeId": str(committee_id)}
                    response = await session.post(
                        "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/viewFinancialEntities",
                        data=data,
                    )
                    response.raise_for_status()
                except Exception as e:
                    logger.error(
                        f"Error fetching entity details for "
                        f"committee {committee_id}: {e}"
                    )
                    return 0

                # Fetch financial entities
                try:
                    params = {
                        "sEcho": "2",
                        "iColumns": "6",
                        "sColumns": "",
                        "iDisplayStart": "0",
                        "iDisplayLength": "100",
                        "mDataProp_0": "transDateStr",
                        "mDataProp_1": "transAmtStr",
                        "mDataProp_2": "totalToDateAmtStr",
                        "mDataProp_3": "entityDTO.entityFullName",
                        "mDataProp_4": "entityDTO.entityAddress",
                        "mDataProp_5": "candidateIssue",
                        "sSearch": "",
                        "bRegex": "false",
                        "iSortCol_0": "0",
                        "sSortDir_0": "asc",
                        "iSortingCols": "1",
                        "_": str(int(time.time() * 1000)),
                    }
                    response = await session.get(
                        "https://cers-ext.mt.gov/CampaignTracker/public/"
                        "searchResults/listViewFinancialEntityResults",
                        params=params,
                    )
                    response.raise_for_status()
                    financial_data = await response.json()

                    # Process and save contribution details
                    contributions = []
                    for contribution in financial_data.get("aaData", []):
                        entity_dto = contribution.get("entityDTO", {})
                        contribution_details = {
                            "committee_id": committee_id,
                            "committee_name": committee.get("committeeName"),
                            "committee_type": committee.get("committeeTypeDescr"),
                            "election_year": year,
                            "transaction_id": contribution.get("transId"),
                            "transaction_date": contribution.get("transDateStr"),
                            "transaction_amount": contribution.get("transAmtStr"),
                            "total_to_date": contribution.get("totalToDateAmtStr"),
                            "contributor_name": entity_dto.get("entityFullName"),
                            "contributor_address": entity_dto.get("entityAddress"),
                            "contributor_city": entity_dto.get("entityCity"),
                            "contributor_state": entity_dto.get("entityState"),
                            "transaction_type": contribution.get("transTypeDesr"),
                            "schedule": contribution.get("scheduleDescr"),
                            "amount_type": contribution.get("amountTypeDescr"),
                            "description": contribution.get("descriptionDescr"),
                            "purpose": contribution.get("purposeDescr"),
                            "occupation": contribution.get("occupationDescr"),
                            "employer": contribution.get("employerDescr"),
                        }
                        contributions.append(contribution_details)

                    # Save to CSV
                    if contributions:
                        output_file = (
                            base_dir
                            / f"{committee_id}_contributions_committees_details.csv"
                        )
                        with open(output_file, "w", newline="", encoding="utf-8") as f:
                            writer = csv.DictWriter(
                                f, fieldnames=contributions[0].keys()
                            )
                            writer.writeheader()
                            writer.writerows(contributions)

                        logger.info(
                            f"Saved {len(contributions)} contributions "
                            f"for committee {committee_id} to {output_file}"
                        )
                        output_files.append(str(output_file))

                    return len(contributions)

                except Exception as e:
                    logger.error(
                        f"Error fetching financial entities for "
                        f"committee {committee_id}: {e}"
                    )

                    return 0

            committee_futures = []

            # Process each committee's data
            for committee in committees:
                committee_id = committee.get("committeeId")
                if not committee_id:
                    continue

                logger.info(f"Processing committee {committee_id}")

                # Fetch entity details
                committee_futures.append(
                    process_committee(committee=committee, committee_id=committee_id)
                )

            committee_completed = await asyncio.gather(*committee_futures)

            for contributions_len in committee_completed:
                total_committees += 1
                total_contributions += int(contributions_len)

            logger.info(
                f"Fetched page {current_page + 1} for year {year}\
                with {len(committees)} committees\
                total contributions {total_contributions}"
            )

            if len(committees) < page_size:
                break

            current_page += 1

    except RequestException as e:
        logger.error(f"Error fetching committees for year {year}: {e}")
        return dg.MaterializeResult(metadata={"dagster/num_committees": 0})
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response for year {year}: {e}")
        return dg.MaterializeResult(metadata={"dagster/num_committees": 0})

    if total_committees > 0:
        return dg.MaterializeResult(
            metadata={
                "dagster/num_committees": total_committees,
                "dagster/output_files": output_files,
            }
        )
    else:
        logger.warning(f"No committee data was fetched for year {year}")
        return dg.MaterializeResult(metadata={"dagster/num_committees": 0})


@dg.asset(partitions_def=mt_contributions_daily_partition, pool="mt_api")
async def mt_fetch_contribution_candidates_for_year(context: dg.AssetExecutionContext):
    """Fetch contribution details for candidates in a specific year."""

    # As the partition key is just the string representation of the year,
    # we can do this cast safely
    year = int(context.partition_key)
    logger = dg.get_dagster_logger(f"mt_fetch_contribution_candidates_for_year_{year}")

    # Create directory for storing candidate contribution data
    base_dir = (
        Path(MT_DATA_PATH_PREFIX) / "contributions_candidates_details" / str(year)
    )
    base_dir.mkdir(parents=True, exist_ok=True)

    session = aiohttp.ClientSession()
    session.headers.update(HEADERS)

    logger.info(f"Fetching candidate contributions for election year {year}")

    # Prepare the POST payload for candidate search
    payload = {
        "financialSearchType": "CONTR",
        "contrSearchTypeCode": "CANDIDATE",
        "contrCanLastName": "",
        "contrCanFirstName": "",
        "contrCommitteeName": "",
        "contributorLastName": "",
        "contributorFirstName": "",
        "contrPartyCode": "",
        "contrCandidateTypeCode": "",
        "contrOfficeCode": "",
        "contrContributorTypeCode": "",
        "contrAmountRangeCode": "",
        "electionYear": year,
        "contrSearchFromDate": "",
        "contrSearchToDate": "",
    }

    total_candidates = 0
    total_contributions = 0
    output_files = []

    try:
        # First get the search page to set up the session
        response = await session.get(
            "https://cers-ext.mt.gov/CampaignTracker/public/search"
        )
        response.raise_for_status()

        # Then make the POST request
        response = await session.post(
            "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/searchFinancials",
            data=payload,
        )
        response.raise_for_status()

        # Now fetch the candidate results
        base_url = "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/listFinancialCandidateResults"
        page_size = 100
        current_page = 0
        total_records = 0

        while True:
            params = {
                "sEcho": "1",
                "iColumns": "5",
                "sColumns": "",
                "iDisplayStart": current_page * page_size,
                "iDisplayLength": page_size,
                "mDataProp_0": "checked",
                "mDataProp_1": "candidateName",
                "mDataProp_2": "electionYear",
                "mDataProp_3": "officeTitle",
                "mDataProp_4": "partyDescr",
                "sSearch": "",
                "bRegex": "false",
                "iSortCol_0": "0",
                "sSortDir_0": "asc",
                "iSortingCols": "1",
                "_": str(int(time.time() * 1000)),
            }

            response = await session.get(base_url, params=params)
            response.raise_for_status()

            data = await response.json()

            if current_page == 0:
                total_records = data.get("iTotalRecords", 0)
                logger.info(f"Total candidates for year {year}: {total_records}")

            candidates = data.get("aaData", [])
            if not candidates:
                break

            async def process_candidate(candidate, candidate_id: str):
                try:
                    data = {"candidateId": str(candidate_id), "committeeId": "0"}
                    response = await session.post(
                        "https://cers-ext.mt.gov/CampaignTracker/public/searchResults/viewFinancialEntities",
                        data=data,
                    )
                    response.raise_for_status()
                except Exception as e:
                    logger.error(
                        f"Error fetching entity details for "
                        f"candidate {candidate_id}: {e}"
                    )
                    return 0

                # Fetch financial entities for the candidate
                try:
                    params = {
                        "sEcho": "2",
                        "iColumns": "6",
                        "sColumns": "",
                        "iDisplayStart": "0",
                        "iDisplayLength": "100",
                        "mDataProp_0": "transDateStr",
                        "mDataProp_1": "transAmtStr",
                        "mDataProp_2": "totalToDateAmtStr",
                        "mDataProp_3": "entityDTO.entityFullName",
                        "mDataProp_4": "entityDTO.entityAddress",
                        "mDataProp_5": "candidateIssue",
                        "sSearch": "",
                        "bRegex": "false",
                        "iSortCol_0": "0",
                        "sSortDir_0": "asc",
                        "iSortingCols": "1",
                        "_": str(int(time.time() * 1000)),
                    }
                    response = await session.get(
                        "https://cers-ext.mt.gov/CampaignTracker/public/"
                        "searchResults/listViewFinancialEntityResults",
                        params=params,
                    )
                    response.raise_for_status()
                    financial_data = await response.json()

                    # Process and save contribution details
                    contributions = []
                    for contribution in financial_data.get("aaData", []):
                        entity_dto = contribution.get("entityDTO", {})
                        contribution_details = {
                            "candidate_id": candidate_id,
                            "candidate_name": candidate.get("candidateName"),
                            "office_title": candidate.get("officeTitle"),
                            "party_descr": candidate.get("partyDescr"),
                            "election_year": year,
                            "transaction_id": contribution.get("transId"),
                            "transaction_date": contribution.get("transDateStr"),
                            "transaction_amount": contribution.get("transAmtStr"),
                            "total_to_date": contribution.get("totalToDateAmtStr"),
                            "contributor_name": entity_dto.get("entityFullName"),
                            "contributor_address": entity_dto.get("entityAddress"),
                            "contributor_city": entity_dto.get("entityCity"),
                            "contributor_state": entity_dto.get("entityState"),
                            "transaction_type": contribution.get("transTypeDesr"),
                            "schedule": contribution.get("scheduleDescr"),
                            "amount_type": contribution.get("amountTypeDescr"),
                            "description": contribution.get("descriptionDescr"),
                            "purpose": contribution.get("purposeDescr"),
                            "occupation": contribution.get("occupationDescr"),
                            "employer": contribution.get("employerDescr"),
                        }
                        contributions.append(contribution_details)

                    # Save to CSV
                    if contributions:
                        output_file = (
                            base_dir
                            / f"{candidate_id}_contributions_candidates_details.csv"
                        )
                        with open(output_file, "w", newline="", encoding="utf-8") as f:
                            writer = csv.DictWriter(
                                f, fieldnames=contributions[0].keys()
                            )
                            writer.writeheader()
                            writer.writerows(contributions)

                        logger.info(
                            f"Saved {len(contributions)} contributions "
                            f"for candidate {candidate_id} to {output_file}"
                        )
                        output_files.append(str(output_file))

                    return len(contributions)

                except Exception as e:
                    logger.error(
                        f"Error fetching financial entities for "
                        f"candidate {candidate_id}: {e}"
                    )
                    return 0

            candidate_futures = []

            # Process each candidate's data
            for candidate in candidates:
                candidate_id = candidate.get("candidateId")
                if not candidate_id:
                    continue

                logger.info(f"Processing candidate {candidate_id}")

                # Fetch entity details
                candidate_futures.append(
                    process_candidate(candidate=candidate, candidate_id=candidate_id)
                )

            candidate_completed = await asyncio.gather(*candidate_futures)

            for contributions_len in candidate_completed:
                total_candidates += 1
                total_contributions += int(contributions_len)

            logger.info(
                f"Fetched page {current_page + 1} for year {year}\
                with {len(candidates)} candidates\
                total contributions {total_contributions}"
            )

            if len(candidates) < page_size:
                break

            current_page += 1

    except RequestException as e:
        logger.error(f"Error fetching candidates for year {year}: {e}")
        return dg.MaterializeResult(metadata={"dagster/num_candidates": 0})
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response for year {year}: {e}")
        return dg.MaterializeResult(metadata={"dagster/num_candidates": 0})

    if total_candidates > 0:
        return dg.MaterializeResult(
            metadata={
                "dagster/num_candidates": total_candidates,
                "dagster/output_files": output_files,
            }
        )
    else:
        logger.warning(f"No candidate data was fetched for year {year}")
        return dg.MaterializeResult(metadata={"dagster/num_candidates": 0})


@dg.asset(
    deps=[mt_fetch_contribution_committees_for_year],
    partitions_def=mt_contributions_daily_partition,
)
def mt_inserting_contribution_committees_details_reports_data(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert contribution details data into the landing table.

    Args:
        pg (ResourceParam[PostgresResource]): The PostgreSQL resource from Dagster.

    Returns:
        MaterializeResult: Contains metadata about the insertion process
    """

    table_name = "mt_contributions_committees_details_landing"

    table_name_identifier = sql.Identifier(table_name)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE election_year = {election_year}"
    ).format(table_name=table_name_identifier, election_year=context.partition_key)

    return mt_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=MT_CONTRIBUTION_COMMITTEES_DETAILS_HEADERS,
        data_validation_callback=(
            lambda row: len(row) == len(MT_CONTRIBUTION_COMMITTEES_DETAILS_HEADERS)
        ),
        category="contributions_committees_details",
        partition_key=context.partition_key,
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[mt_fetch_contribution_candidates_for_year],
    partitions_def=mt_contributions_daily_partition,
)
def mt_inserting_contribution_candidates_details_reports_data(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert candidate contribution details data into the landing table.

    Args:
        pg (ResourceParam[PostgresResource]): The PostgreSQL resource from Dagster.

    Returns:
        MaterializeResult: Contains metadata about the insertion process
    """

    table_name = "mt_contributions_candidates_details_landing"

    table_name_identifier = sql.Identifier(table_name)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE election_year = {election_year}"
    ).format(table_name=table_name_identifier, election_year=context.partition_key)

    return mt_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=MT_CONTRIBUTION_CANDIDATES_DETAILS_HEADERS,
        data_validation_callback=(
            lambda row: len(row) == len(MT_CONTRIBUTION_CANDIDATES_DETAILS_HEADERS)
        ),
        category="contributions_candidates_details",
        partition_key=context.partition_key,
        truncate_query=truncate_query,
    )


assets = [
    mt_fetch_contribution_committees_for_year,
    mt_fetch_contribution_candidates_for_year,
    mt_fetch_candidates_data,
    mt_fetch_committees_data,
    mt_inserting_candidates_reports_data,
    mt_inserting_committees_reports_data,
    mt_inserting_contribution_committees_details_reports_data,
    mt_inserting_contribution_candidates_details_reports_data,
]

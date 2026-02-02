import csv
import glob
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup, Tag
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_delete_by_year_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants for New Mexico
NM_DATA_PATH_PREFIX = "./states/new_mexico"
NM_BASE_URL = "https://www.cfis.state.nm.us"
NM_BASE_LOGIN_URL = "https://login.cfis.sos.state.nm.us"
NM_DATA_DOWNLOAD_URL = f"{NM_BASE_URL}/media/CFIS_Data_Download.aspx"
NM_CANDIDATE_SEARCH_URL = f"{NM_BASE_LOGIN_URL}/api///Organization/SearchCandidates"
NM_COMMITTEE_SEARCH_URL = f"{NM_BASE_LOGIN_URL}/api///Organization/SearchCommittees"
NM_COMMITTEE_DETAILS_URL = (
    f"{NM_BASE_LOGIN_URL}/api///Organization/GetCommitteeInformation"
)

NM_TRANSACTION_TYPES = {
    "CON": "contributions",
    "EXP": "expenditures",
}

# PAC Transaction Columns
NM_PAC_TRANSACTION_HEADERS = [
    "pac_name",
    "description",
    "is_contribution",
    "is_anonymous",
    "amount",
    "date_contribution",
    "memo",
    "contrib_expenditure_description",
    "contrib_expenditure_first_name",
    "contrib_expenditure_middle_name",
    "contrib_expenditure_last_name",
    "suffix",
    "company_name",
    "address",
    "city",
    "state",
    "zip",
    "occupation",
    "filing_period",
    "date_added",
]

# Candidate Transaction Columns
NM_CANDIDATE_TRANSACTION_HEADERS = [
    "first_name",
    "last_name",
    "description",
    "is_contribution",
    "is_anonymous",
    "amount",
    "date_contribution",
    "memo",
    "contrib_expenditure_description",
    "contrib_expenditure_first_name",
    "contrib_expenditure_middle_name",
    "contrib_expenditure_last_name",
    "suffix",
    "company_name",
    "address",
    "city",
    "state",
    "zip",
    "occupation",
    "filing_period",
    "date_added",
]

# Candidate Headers
NM_CANDIDATE_HEADERS = [
    "candidate_name",
    "office_name",
    "election_year",
    "election_year_str",
    "party",
    "district",
    "jurisdiction",
    "finance_type",
    "status",
    "incumbent",
    "id_number",
    "treasurer_name",
    "candidate_address",
    "registration_date",
    "last_filing_date",
    "public_phone_number",
    "political_party_committee_name",
    "candidate_status",
    "jurisdiction_type",
    "officer_holder_status",
    "election_name",
    "number_of_candidates",
    "office_id",
    "election_id",
    "district_id",
    "election_cycle_id",
    "office_type",
    "jurisdiction_id",
    "registration_id",
    "finance_status",
    "row_number",
    "total_rows",
    "unregistered_candidate",
    "state_id",
    "total_contributions",
    "total_expenditures",
    "member_id",
    "termination_date",
    "candidate_email",
    "committee_email",
    "is_compliant",
    "compliant_status",
    "is_legacy",
]

# Committee Headers
NM_COMMITTEE_HEADERS = [
    "committee_name",
    "committee_type",
    "committee_type_code",
    "display_id",
    "committee_address",
    "qualified_date",
    "last_filing_date",
    "phone",
    "election_year",
    "status",
    "total_contributions",
    "total_expenditures",
    "pac_type",
    "party",
    "id_number",
    "state_id",
    "date_registered",
    "treasurer",
    "office_id",
    "election_id",
    "district_id",
    "election_cycle_id",
    "registration_id",
    "row_number",
    "total_rows",
    "member_id",
    "termination_date",
    "committee_subtype",
    "email",
    "compliant_status",
]

NM_CONTRIBUTIONS_HEADERS = [
    "org_id",
    "transaction_amount",
    "transaction_date",
    "last_name",
    "first_name",
    "middle_name",
    "prefix",
    "suffix",
    "contributor_address_line_1",
    "contributor_address_line_2",
    "contributor_city",
    "contributor_state",
    "contributor_zip_code",
    "description",
    "check_number",
    "transaction_id",
    "filed_date",
    "election",
    "report_name",
    "start_of_period",
    "end_of_period",
    "contributor_code",
    "contribution_type",
    "report_entity_type",
    "committee_name",
    "candidate_last_name",
    "candidate_first_name",
    "candidate_middle_name",
    "candidate_prefix",
    "candidate_suffix",
    "amended",
    "contributor_employer",
    "contributor_occupation",
    "occupation_comment",
    "employment_information_requested",
]

NM_EXPENDITURES_HEADERS = [
    "org_id",
    "expenditure_amount",
    "expenditure_date",
    "payee_last_name",
    "payee_first_name",
    "payee_middle_name",
    "payee_prefix",
    "payee_suffix",
    "payee_address_1",
    "payee_address_2",
    "payee_city",
    "payee_state",
    "payee_zip_code",
    "description",
    "expenditure_id",
    "filed_date",
    "election",
    "report_name",
    "start_of_period",
    "end_of_period",
    "purpose",
    "expenditure_type",
    "reason",
    "stance",
    "report_entity_type",
    "committee_name",
    "candidate_last_name",
    "candidate_first_name",
    "candidate_middle_name",
    "candidate_prefix",
    "candidate_suffix",
    "amended",
]


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
    "Host": "www.cfis.state.nm.us",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": ('"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'),
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
}

# ===== Partition Definition =====
# Unified partition definition starting from 2004 to cover all data types
nm_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=datetime(2004, 1, 1),
    end_offset=1,
)


def get_viewstate_and_years(
    session: requests.Session,
) -> tuple[str, str, str, list[str]]:
    """Get VIEWSTATE, VIEWSTATEGENERATOR, EVENTVALIDATION and
    list of years from the data download page.

    This function makes a GET request to the New Mexico SOS portal's data download page
    and extracts the necessary form fields and available years for data download.

    Args:
        session (requests.Session): An active requests session for making HTTP requests.

    Returns:
        tuple[str, str, str, list[str]]: A tuple containing:
            - VIEWSTATE value from the form
            - VIEWSTATEGENERATOR value from the form
            - EVENTVALIDATION value from the form
            - List of available years for data download

    Raises:
        ValueError: If any of the required form fields cannot be found in the HTML.
    """
    response = session.get(NM_DATA_DOWNLOAD_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    # Get VIEWSTATE
    viewstate_input = soup.find("input", {"id": "__VIEWSTATE"})
    if not isinstance(viewstate_input, Tag) or not viewstate_input.get("value"):
        raise ValueError("Could not find VIEWSTATE input")
    viewstate = str(viewstate_input["value"])

    # Get VIEWSTATEGENERATOR
    viewstate_generator_input = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    if not isinstance(
        viewstate_generator_input, Tag
    ) or not viewstate_generator_input.get("value"):
        raise ValueError("Could not find VIEWSTATEGENERATOR input")
    viewstate_generator = str(viewstate_generator_input["value"])

    # Get EVENTVALIDATION
    event_validation_input = soup.find("input", {"id": "__EVENTVALIDATION"})
    if not isinstance(event_validation_input, Tag) or not event_validation_input.get(
        "value"
    ):
        raise ValueError("Could not find EVENTVALIDATION input")
    event_validation = str(event_validation_input["value"])

    # Get years from the dropdown
    years: list[str] = []
    year_select = soup.find(
        "select", {"id": "ctl00_ContentPlaceHolder1_header1_ddlFilePeriodYear"}
    )
    if isinstance(year_select, Tag):
        for option in year_select.find_all("option"):
            if (
                isinstance(option, Tag)
                and option.get("value")
                and option["value"] != "0"
            ):
                years.append(str(option["value"]))

    return viewstate, viewstate_generator, event_validation, years


def download_transactions(
    session: requests.Session,
    viewstate: str,
    viewstate_generator: str,
    event_validation: str,
    year: str,
    entity_type: str,
    output_path: Path,
) -> None:
    """Download transactions data for a specific year and entity type.

    This function makes a POST request to download transaction data
    (contributions or expenditures)
    for either candidates or PACs for a specific year.

    Args:
        session (requests.Session): An active requests session for making HTTP requests.
        viewstate (str): The VIEWSTATE value from the form.
        viewstate_generator (str): The VIEWSTATEGENERATOR value from the form.
        event_validation (str): The EVENTVALIDATION value from the form.
        year (str): The election year to download data for.
        entity_type (str): The type of entity to download data for
        ("Candidates" or "PACs").
        output_path (Path): The path where the downloaded CSV file will be saved.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
    """
    cookie_string = "; ".join(
        [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
    )

    payload = {
        "__VIEWSTATE": viewstate,
        "__VIEWSTATEGENERATOR": viewstate_generator,
        "__EVENTVALIDATION": event_validation,
        "ctl00$ContentPlaceHolder1$header1$ddlCSVSelect": "Transactions",
        "ctl00$ContentPlaceHolder1$header1$ddlRegisrationYear": "0",
        "ctl00$ContentPlaceHolder1$header1$ddlViewBy": "Lobbyist",
        "ctl00$ContentPlaceHolder1$header1$hfFilePeriodFilter": "ALL",
        "ctl00$ContentPlaceHolder1$header1$ddlLookFor": entity_type,
        "ctl00$ContentPlaceHolder1$header1$ddlFilePeriodYear": str(year),
        "ctl00$ContentPlaceHolder1$header1$ddlFPCan": "ALL",
        "ctl00$ContentPlaceHolder1$header1$Button1": "Download Data",
        "ctl00$ContentPlaceHolder1$header1$hfLobbyistFilingPeriod": "ALL",
        "ctl00$ContentPlaceHolder1$header1$ddlTransRegYear": "0",
        "ctl00$ContentPlaceHolder1$header1$ddlFPLob": "ALL",
    }

    response = session.post(
        NM_DATA_DOWNLOAD_URL,
        data=payload,
        headers={
            **HEADERS,
            "cookie": cookie_string,
        },
    )

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def nm_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    category: str,
    data_files: list[str],
    truncate_query: sql.Composed,
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

    if data_files is None:
        # Use automatic file discovery for non-contributions
        base_path = Path(NM_DATA_PATH_PREFIX)
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
    partitions_def=nm_yearly_partition,
)
def nm_fetch_candidates_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches candidate data from New Mexico's campaign finance system for a specific
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
        # Create output directory
        output_dir = Path(NM_DATA_PATH_PREFIX) / "candidates"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create a session
        session = requests.Session()

        # Headers for the request
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://login.cfis.sos.state.nm.us",
            "referer": "https://login.cfis.sos.state.nm.us/",
            "user-agent": HEADERS["User-Agent"],
        }

        logger.info(f"Start download for year: {year}")
        page_number = 1
        all_candidates = []

        while True:
            # Prepare the payload
            payload = {
                "ElectionYear": str(year),
                "Party": None,
                "OfficeSought": None,
                "JurisdictionType": None,
                "Jurisdiction": None,
                "FinanceType": None,
                "TransactionType": None,
                "TransactionAmount": None,
                "DistrictId": None,
                "IsCompliance": None,
                "pageNumber": page_number,
                "pageSize": 50,
                "sortDir": "ASC",
                "sortedBy": "CandidateName",
            }

            # Make the request
            response = session.post(
                NM_CANDIDATE_SEARCH_URL, json=payload, headers=headers
            )
            response.raise_for_status()

            # Parse the response
            data = response.json()

            if not data:
                break

            all_candidates.extend(data)

            # Check if we've reached the last page
            if len(data) < 10:
                break

            page_number += 1

        # Save to CSV
        output_path = output_dir / f"{year}_candidates.csv"
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NM_CANDIDATE_HEADERS)
            writer.writeheader()

            for candidate in all_candidates:
                # Explicitly map each field
                processed_candidate = {
                    "candidate_name": str(candidate.get("CandidateName", "")),
                    "office_name": str(candidate.get("OfficeName", "")),
                    "election_year": str(candidate.get("ElectionYear", "")),
                    "election_year_str": str(candidate.get("ElectionYearStr", "")),
                    "party": str(candidate.get("Party", "")),
                    "district": str(candidate.get("District", "")),
                    "jurisdiction": str(candidate.get("Jurisdiction", "")),
                    "finance_type": str(candidate.get("FinanceType", "")),
                    "status": str(candidate.get("Status", "")),
                    "incumbent": str(candidate.get("Incumbent", "")),
                    "id_number": str(candidate.get("IdNumber", "")),
                    "treasurer_name": str(candidate.get("TreasurerName", "")),
                    "candidate_address": str(candidate.get("CandidateAddress", "")),
                    "registration_date": str(candidate.get("RegistrationDate", "")),
                    "last_filing_date": str(candidate.get("LastFilingDate", "")),
                    "public_phone_number": str(candidate.get("PublicPhoneNumber", "")),
                    "political_party_committee_name": (
                        str(candidate.get("PoliticalPartyCommitteeName", ""))
                    ),
                    "candidate_status": str(candidate.get("CandidateStatus", "")),
                    "jurisdiction_type": str(candidate.get("JurisdictionType", "")),
                    "officer_holder_status": (
                        str(candidate.get("OfficerHolderStatus", ""))
                    ),
                    "election_name": str(candidate.get("ElectionName", "")),
                    "number_of_candidates": (
                        str(candidate.get("NumberOfCandidates", ""))
                    ),
                    "office_id": str(candidate.get("OfficeId", "")),
                    "election_id": str(candidate.get("ElectionId", "")),
                    "district_id": str(candidate.get("DistrictId", "")),
                    "election_cycle_id": str(candidate.get("ElectionCycleId", "")),
                    "office_type": str(candidate.get("OfficeType", "")),
                    "jurisdiction_id": str(candidate.get("JurisdictionId", "")),
                    "registration_id": str(candidate.get("RegistrationId", "")),
                    "finance_status": str(candidate.get("FinanceStatus", "")),
                    "row_number": str(candidate.get("RowNumber", "")),
                    "total_rows": str(candidate.get("TotalRows", "")),
                    "unregistered_candidate": (
                        str(candidate.get("UnregisteredCandidate", ""))
                    ),
                    "state_id": str(candidate.get("StateId", "")),
                    "total_contributions": str(candidate.get("TotalContributions", "")),
                    "total_expenditures": str(candidate.get("TotalExpenditures", "")),
                    "member_id": str(candidate.get("MemberId", "")),
                    "termination_date": str(candidate.get("TerminationDate", "")),
                    "candidate_email": str(candidate.get("CandidateEmail", "")),
                    "committee_email": str(candidate.get("CommitteeEmail", "")),
                    "is_compliant": str(candidate.get("IsCompliant", "")),
                    "compliant_status": str(candidate.get("CompliantStatus", "")),
                    "is_legacy": str(candidate.get("IsLegacy", "")),
                }

                # Only write row if it has at least one non-empty value
                if any(value.strip() for value in processed_candidate.values()):
                    writer.writerow(processed_candidate)

        logger.info(f"Downloaded {len(all_candidates)} candidates for year {year}")

    except Exception as e:
        logger.error(f"Error fetching New Mexico candidates for year {year}: {e!s}")
        raise


@dg.asset(
    partitions_def=nm_yearly_partition,
)
def nm_fetch_committees_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches committee data from New Mexico's campaign finance system for a specific
    year. Downloads committee details for each committee found in the search results.
    This asset is partitioned by year, fetching data for each year starting from 2004.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2004 when committees data became available
    if year < 2004:
        logger.info(
            f"Committees data not available for year {year} (before 2004), skipping"
        )
        return

    try:
        # Create output directory
        output_dir = Path(NM_DATA_PATH_PREFIX) / "committees"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create a session
        session = requests.Session()
        session.verify = False

        # Headers for the request
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://login.cfis.sos.state.nm.us",
            "referer": "https://login.cfis.sos.state.nm.us/",
            "user-agent": HEADERS["User-Agent"],
        }
        session.headers.update(headers)

        logger.info(f"Start download for year: {year}")
        page_number = 1
        total_rows = 0
        all_committees = []

        while True:
            # Prepare the payload
            payload = {
                "ElectionYear": str(year),
                "party": "",
                "committeeType": "",
                "transactionType": "",
                "transactionAmount": None,
                "ballotQuestions": None,
                "stance": "",
                "pacType": "",
                "status": "",
                "BallotQuestionOnly": None,
                "IsCompliance": None,
                "pageNumber": page_number,
                "pageSize": 50,
                "sortDir": "asc",
                "sortedBy": "",
            }

            # Make the request
            response = session.post(NM_COMMITTEE_SEARCH_URL, json=payload)
            response.raise_for_status()

            # Parse the response
            data = response.json()

            if not data:
                break

            # Get total rows from first page
            if total_rows == 0 and data:
                total_rows = int(data[0].get("TotalRows", 0))
                logger.info(f"Total committees for year {year}: {total_rows}")

            # Fetch details for each committee
            for committee in data:
                member_id = committee.get("IdNumber")
                if not member_id:
                    continue

                logger.info(f"Fetching details for committee {member_id}")

                # Get committee details
                details_response = session.get(
                    NM_COMMITTEE_DETAILS_URL, params={"memberId": member_id}
                )
                details_response.raise_for_status()

                committee_details = details_response.json()
                if committee_details:
                    # Process each committee detail record
                    for detail in committee_details:
                        # Clean up the address field
                        if "CommitteeAddress" in detail:
                            detail["CommitteeAddress"] = detail[
                                "CommitteeAddress"
                            ].replace("<br>", ", ")

                        # Add the committee to our collection
                        all_committees.append(detail)

            # Check if we've processed all pages
            if page_number * 50 >= total_rows:
                break

            page_number += 1

        # Save to CSV
        output_path = output_dir / f"{year}_committees.csv"
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NM_COMMITTEE_HEADERS)
            writer.writeheader()

            for committee in all_committees:
                # Explicitly map each field
                processed_committee = {
                    "committee_name": str(committee.get("CommitteeName", "")),
                    "committee_type": str(committee.get("CommitteeType", "")),
                    "committee_type_code": str(committee.get("CommitteeTypeCode", "")),
                    "display_id": str(committee.get("DisplayID", "")),
                    "committee_address": str(committee.get("CommitteeAddress", "")),
                    "qualified_date": str(committee.get("QualifiedDate", "")),
                    "last_filing_date": str(committee.get("LastFilingDate", "")),
                    "phone": str(committee.get("Phone", "")),
                    "election_year": str(committee.get("ElectionYear", "")),
                    "status": str(committee.get("Status", "")),
                    "total_contributions": str(committee.get("TotalContributions", "")),
                    "total_expenditures": str(committee.get("TotalExpenditures", "")),
                    "pac_type": str(committee.get("PacType", "")),
                    "party": str(committee.get("Party", "")),
                    "id_number": str(committee.get("IdNumber", "")),
                    "state_id": str(committee.get("StateID", "")),
                    "date_registered": str(committee.get("DateRegistered", "")),
                    "treasurer": str(committee.get("Treasurer", "")),
                    "office_id": str(committee.get("OfficeId", "")),
                    "election_id": str(committee.get("ElectionId", "")),
                    "district_id": str(committee.get("DistrictId", "")),
                    "election_cycle_id": str(committee.get("ElectionCycleId", "")),
                    "registration_id": str(committee.get("RegistrationId", "")),
                    "row_number": str(committee.get("RowNumber", "")),
                    "total_rows": str(committee.get("TotalRows", "")),
                    "member_id": str(committee.get("MemberID", "")),
                    "termination_date": str(committee.get("TerminationDate", "")),
                    "committee_subtype": str(committee.get("CommitteeSubtype", "")),
                    "email": str(committee.get("Email", "")),
                    "compliant_status": str(committee.get("CompliantStatus", "")),
                }

                # Only write row if it has at least one non-empty value
                if any(value.strip() for value in processed_committee.values()):
                    writer.writerow(processed_committee)

        logger.info(f"Downloaded {len(all_committees)} committees for year {year}")

    except Exception as e:
        logger.error(f"Error fetching New Mexico committees for year {year}: {e!s}")
        raise


@dg.asset(
    partitions_def=nm_yearly_partition,
)
def nm_fetch_transactions_candidate_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches transaction data for candidates from New Mexico's campaign finance system
    for a specific year. This asset is partitioned by year, fetching data for each year
    starting from 2010.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2010 when transaction data became available
    if year < 2010:
        logger.info(
            f"Transaction data not available for year {year} (before 2010), skipping"
        )
        return

    try:
        # Create output directory
        output_dir = Path(NM_DATA_PATH_PREFIX) / "transactions_candidate"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create a session
        session = requests.Session()

        # Get VIEWSTATE, VIEWSTATEGENERATOR, EVENTVALIDATION and years
        viewstate, viewstate_generator, event_validation, years = (
            get_viewstate_and_years(session)
        )

        # Only process the current year
        if str(year) in years:
            output_path = output_dir / f"{year}_transactions_candidate.csv"
            download_transactions(
                session=session,
                viewstate=viewstate,
                viewstate_generator=viewstate_generator,
                event_validation=event_validation,
                year=str(year),
                entity_type="Candidates",
                output_path=output_path,
            )
            logger.info(f"Downloaded transactions for year {year}")
        else:
            logger.info(f"Year {year} not available for candidate transactions")

    except Exception as e:
        logger.error(
            f"Error fetching New Mexico candidate\
            transactions for year {year}: {e!s}"
        )
        raise


@dg.asset(
    partitions_def=nm_yearly_partition,
)
def nm_fetch_transactions_pac_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches transaction data for PACs from New Mexico's campaign finance system
    for a specific year. This asset is partitioned by year, fetching data for each year
    starting from 2010.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2010 when transaction data became available
    if year < 2010:
        logger.info(
            f"Transaction data not available for year {year} (before 2010), skipping"
        )
        return

    try:
        # Create output directory
        output_dir = Path(NM_DATA_PATH_PREFIX) / "transactions_pac"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create a session
        session = requests.Session()

        # Get VIEWSTATE, VIEWSTATEGENERATOR, EVENTVALIDATION and years
        viewstate, viewstate_generator, event_validation, years = (
            get_viewstate_and_years(session)
        )
        # Only process the current year
        if str(year) in years:
            output_path = output_dir / f"{year}_transactions_pac.csv"
            download_transactions(
                session=session,
                viewstate=viewstate,
                viewstate_generator=viewstate_generator,
                event_validation=event_validation,
                year=str(year),
                entity_type="PACs",
                output_path=output_path,
            )
            logger.info(f"Downloaded transactions for year {year}")
        else:
            logger.info(f"Year {year} not available for PAC transactions")

    except Exception as e:
        logger.error(
            f"Error fetching New Mexico PAC\
            transactions for year {year}: {e!s}"
        )
        raise


@dg.asset(
    partitions_def=nm_yearly_partition,
)
def nm_fetch_contributions_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches and downloads raw campaign finance data for New Mexico from the official
    state website (https://login.cfis.sos.state.nm.us/#/dataDownload)
    for a specific year.

    This asset is partitioned by year, fetching data for each year starting from 2020.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when contributions data became available
    if year < 2020:
        logger.info(
            f"Contributions data not available for year {year} (before 2020), skipping"
        )
        return

    logger.info(f"Downloading contributions data for {year}")

    url = (
        f"{NM_BASE_LOGIN_URL}/api/DataDownload/GetCSVDownloadReport?year={year}"
        f"&transactionType=CON"
        f"&reportFormat=csv"
        f"&fileName=CON_{year}.csv"
    )

    logger.info(f"Downloading contributions data for {year} with url {url}")
    category_dir = Path(NM_DATA_PATH_PREFIX, "contributions")
    category_dir.mkdir(parents=True, exist_ok=True)
    file_path = category_dir / f"{year}_contributions.csv"

    stream_download_file_to_path(request_url=url, file_save_path=file_path)


@dg.asset(
    partitions_def=nm_yearly_partition,
)
def nm_fetch_expenditures_raw_data(context: dg.AssetExecutionContext) -> None:
    """
    Fetches and downloads raw expenditure data for New Mexico from the official
    state website for a specific year.

    This asset is partitioned by year, fetching data for each year starting from 2020.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when expenditures data became available
    if year < 2020:
        logger.info(
            f"Expenditures data not available for year {year} (before 2020), skipping"
        )
        return

    logger.info(f"Downloading expenditures data for {year}")

    url = (
        f"{NM_BASE_LOGIN_URL}/api/DataDownload/GetCSVDownloadReport?year={year}"
        f"&transactionType=EXP"
        f"&reportFormat=csv"
        f"&fileName=EXP_{year}.csv"
    )

    logger.info(f"Downloading expenditures data for {year} with url {url}")

    category_dir = Path(NM_DATA_PATH_PREFIX, "expenditures")

    category_dir.mkdir(parents=True, exist_ok=True)

    file_path = category_dir / f"{year}_expenditures.csv"

    stream_download_file_to_path(request_url=url, file_save_path=file_path)


# ===== Insert Assets =====
@dg.asset(
    deps=[nm_fetch_candidates_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_candidates_to_landing_table(
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
                "dagster/table_name": "nm_candidates_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all candidate files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "candidates"
    candidate_files = list(data_dir.glob(f"{year}_candidates.csv"))

    if not candidate_files:
        logger.info(f"No candidate files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_candidates_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_candidates_landing"

    # Create a truncate query that only deletes data for the specific year
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="election_year", year=year
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_CANDIDATE_HEADERS,
        data_validation_callback=lambda row: len(row) == len(NM_CANDIDATE_HEADERS),
        category="candidates",
        data_files=[str(f) for f in candidate_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[nm_fetch_committees_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_committees_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert committee data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2004 when committees data became available
    if year < 2004:
        logger.info(
            f"Committees data not available for year {year} (before 2004), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_committees_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all committee files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "committees"
    committee_files = list(data_dir.glob(f"{year}_committees.csv"))

    if not committee_files:
        logger.info(f"No committee files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_committees_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_committees_landing"

    # Create a truncate query that only deletes data for the specific year
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="election_year", year=year
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_COMMITTEE_HEADERS,
        data_validation_callback=lambda row: len(row) == len(NM_COMMITTEE_HEADERS),
        category="committees",
        data_files=[str(f) for f in committee_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[nm_fetch_transactions_candidate_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_transactions_candidate_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert candidate transaction data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when transaction data became available
    if year < 2020:
        logger.info(
            f"Transaction data not available for year {year} (before 2020), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_transactions_candidate_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all transaction candidate files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "transactions_candidate"
    transaction_files = list(data_dir.glob(f"{year}_transactions_candidate.csv"))

    if not transaction_files:
        logger.info(f"No transaction candidate files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_transactions_candidate_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_transactions_candidate_landing"

    # Create a truncate query that only deletes data for the specific year
    # Use date_added field for year-based filtering
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="date_added", year=year, mode="extract"
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_CANDIDATE_TRANSACTION_HEADERS,
        data_validation_callback=(
            lambda row: len(row) == len(NM_CANDIDATE_TRANSACTION_HEADERS)
        ),
        category="transactions_candidate",
        data_files=[str(f) for f in transaction_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[nm_fetch_transactions_pac_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_transactions_pac_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert PAC transaction data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when transaction data became available
    if year < 2020:
        logger.info(
            f"Transaction data not available for year {year} (before 2020), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_transactions_pac_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all transaction PAC files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "transactions_pac"
    transaction_files = list(data_dir.glob(f"{year}_transactions_pac.csv"))

    if not transaction_files:
        logger.info(f"No transaction PAC files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_transactions_pac_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_transactions_pac_landing"

    # Create a truncate query that only deletes data for the specific year
    # Use date_added field for year-based filtering
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="date_added", year=year, mode="extract"
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_PAC_TRANSACTION_HEADERS,
        data_validation_callback=(
            lambda row: len(row) == len(NM_PAC_TRANSACTION_HEADERS)
        ),
        category="transactions_pac",
        data_files=[str(f) for f in transaction_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[nm_fetch_contributions_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_raw_contributions_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert contributions data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when contributions data became available
    if year < 2020:
        logger.info(
            f"Contributions data not available for year {year} (before 2020), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_contributions_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all contribution CSV files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "contributions"
    contribution_files = list(data_dir.glob(f"{year}_contributions.csv"))

    if not contribution_files:
        logger.info(f"No contribution files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_contributions_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_contributions_landing"

    # Create a truncate query that only deletes data for the specific year
    # Use transaction_date for year-based filtering
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="transaction_date", year=year, mode="extract"
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_CONTRIBUTIONS_HEADERS,
        data_validation_callback=lambda row: len(row) == len(NM_CONTRIBUTIONS_HEADERS),
        category="contributions",
        data_files=[str(f) for f in contribution_files],
        truncate_query=truncate_query,
    )


@dg.asset(
    deps=[nm_fetch_expenditures_raw_data],
    partitions_def=nm_yearly_partition,
)
def nm_insert_raw_expenditures_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert expenditures data into the landing table for a specific year.
    Deletes existing data for the year before inserting new data.
    """
    logger = context.log
    year = int(context.partition_key)

    # Skip years before 2020 when expenditures data became available
    if year < 2020:
        logger.info(
            f"Expenditures data not available for year {year} (before 2020), skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_expenditures_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "skipped_before_availability",
            }
        )

    # Get all expenditure CSV files for the specific year
    data_dir = Path(NM_DATA_PATH_PREFIX) / "expenditures"
    expenditure_files = list(data_dir.glob(f"{year}_expenditures.csv"))

    if not expenditure_files:
        logger.info(f"No expenditure files found for year {year}")
        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": "nm_expenditures_landing",
                "dagster/row_count": 0,
                "dagster/year": year,
                "dagster/status": "no_files_found",
            }
        )

    table_name = "nm_expenditures_landing"

    # Create a truncate query that only deletes data for the specific year
    # Use expenditure_date for year-based filtering
    truncate_query = get_sql_delete_by_year_query(
        table_name=table_name, column_name="expenditure_date", year=year, mode="extract"
    )

    return nm_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=NM_EXPENDITURES_HEADERS,
        data_validation_callback=lambda row: len(row) == len(NM_EXPENDITURES_HEADERS),
        category="expenditures",
        data_files=[str(f) for f in expenditure_files],
        truncate_query=truncate_query,
    )

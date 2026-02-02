"""
Michigan Campaign Finance Reporting assets.

This module implements Dagster assets that download, process, and load Michigan
campaign finance contribution data into a PostgreSQL database.

The data is sourced from the Michigan Board of Elections (BOE) Campaign Finance
Reporting (CFR) system, where it's provided as annual archive files. This data is only
available from 2020-2025 (prior to April 2025) as the new system is currently now in
place which handle the newer data.
https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data/

The newer data are available through a search from "Contributions Analysis",
and "All Committee" search through MiTN (Michigan Transparency Network).

For contributions search:
https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch
For committee search:
https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeCommitteePublicSearch
"""

import csv
import glob
import os
import random
import sys
import time
import typing
from datetime import datetime
from pathlib import Path

import dagster as dg
import py7zr
import requests
from bs4 import BeautifulSoup, Tag
from psycopg import sql

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants
MI_CONTRIBUTIONS_LANDING_TABLE = "mi_contributions_landing"
MI_DATA_PATH_PREFIX = "./states/michigan"

# Column names for the contributions landing table
MI_CONTRIBUTIONS_COLUMNS = [
    "doc_seq_no",
    "page_no",
    "contribution_id",
    "cont_detail_id",
    "doc_stmnt_year",
    "doc_type_desc",
    "com_legal_name",
    "common_name",
    "cfr_com_id",
    "com_type",
    "can_first_name",
    "can_last_name",
    "contribtype",
    "f_name",
    "l_name",
    "address",
    "city",
    "state",
    "zip",
    "occupation",
    "employer",
    "received_date",
    "amount",
    "aggregate",
    "extra_desc",
    "runtime",
]

# Set the CSV field size limit to maximum to handle large fields
csv.field_size_limit(sys.maxsize)

MI_LEGACY_FIRST_YEAR_AVAILABLE = 2020
MI_LEGACY_LAST_YEAR_AVAILABLE_INCLUSIVE = 2025

# Updated URL base for Michigan campaign finance data
MI_LEGACY_CFR_BASE_URL = "https://www.michigan.gov/sos/-/media/Project/Websites/sos/Elections/Disclosure/MiTN/Legacy-Data"

MI_MITN_FIRST_DATE_AVAILABLE = datetime(2025, 4, 1)

MI_MITN_CONTRTIBUTIONS_ANALYSIS_BASE_URL = "https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch"


# User agent headers to avoid 403 Forbidden errors
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,"
    "application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

MI_MITN_CONTRIBUTIONS_SEARCH_REQUEST_HEADER = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/91.0.4472.124 Safari/537.36",
    "content-type": "application/x-www-form-urlencoded",
    "hx-current-url": "https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch",
    "hx-request": "true",
    "hx-target": "search-results",
    "pragma": "no-cache",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "Referer": "https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch",
}


def mi_create_contributions_request_body(
    start_date: str, end_date: str, page: int | None = 1
):
    """
    Create request body to search for contributions with MiTN
    "Contributions Analysis" function.

    Params:
    start_date: start date of the filter formatted as YYYY-MM-DD
    end_date: end date of the filter formatted as YYYY-MM-DD
    page: page number of the result
    """

    return {
        "form.committeeName": "",
        "form.committeeId": "",
        "form.committeeType": "",
        "form.committeeCandidateLastName": "",
        "form.committeeOfficeTitle": "",
        "form.committeePoliticalParty": "",
        "form.campaignStatementYear": "",
        "form.campaignCoverageYearBegin": "",
        "form.campaignCoverageYearEnd": "",
        "form.campaignStatementType": "",
        "form.campaignStatementName": "",
        "form.contributionType": "individual",
        "form.contributionAmountGreaterThan": "",
        "form.contributionAmountLessThan": "",
        "form.contributionSchedule": "",
        "form.contributionDateBegin": start_date,
        "form.contributionDateEnd": end_date,
        "form.contributorLastNamePac": "",
        "form.contributorFirstName": "",
        "form.contributorAddress": "",
        "form.contributorCity": "",
        "form.contributorState": "",
        "form.contributorZip": "",
        "form.contributorEmployer": "",
        "form.contributorOccupation": "",
        "perPage": "100",
        "gotoPage": page,
        "option": "ContributionAnalysisNew",
    }


mi_daily_partition = dg.DailyPartitionsDefinition(
    start_date=MI_MITN_FIRST_DATE_AVAILABLE,
    timezone="America/New_York",
    fmt="%Y-%m-%d",
)


@dg.asset(partitions_def=mi_daily_partition, pool="mi_api")
def mi_fetch_contributions(context: dg.AssetExecutionContext):
    """
    This asset will fetch contributions done within the current partition
    from the MiTN "Contributions Analysis" search
    """
    req_session = requests.session()

    base_output_path = Path(MI_DATA_PATH_PREFIX)
    partition_result_dir = base_output_path / context.partition_key
    partition_result_dir.mkdir(parents=True, exist_ok=True)

    partition_result_path = partition_result_dir / "contributions.csv"

    base_path_res = req_session.get(
        MI_MITN_CONTRTIBUTIONS_ANALYSIS_BASE_URL, headers=REQUEST_HEADERS
    )
    base_path_res.raise_for_status()

    context.log.info(f"Cookies: {base_path_res.cookies.get_dict()}")

    page_number = 1
    with open(partition_result_path, "w") as output_path:
        output_writer = csv.writer(output_path)

        while True:
            data_count = 0

            context.log.info(
                f"Getting 100 data from {context.partition_key} date,"
                f" page number {page_number}"
            )

            data_page = req_session.post(
                MI_MITN_CONTRTIBUTIONS_ANALYSIS_BASE_URL + "&action=search",
                data=mi_create_contributions_request_body(
                    start_date=context.partition_key,
                    end_date=context.partition_key,
                    page=page_number,
                ),
                headers=MI_MITN_CONTRIBUTIONS_SEARCH_REQUEST_HEADER,
            )
            data_page_content_type = data_page.headers.get("content-type") or ""

            if not data_page.ok or not data_page_content_type.startswith("text/html"):
                context.log.warning(
                    f"Status Code or content-type from MiTN not okay!"
                    f" ({data_page.status_code}, {data_page_content_type})"
                )
                break

            print(f"--PAGE {page_number}-------------------------------------")
            print(f"Raw page: {data_page.text}")
            print("----------------------------------------------------------")
            soup = BeautifulSoup(data_page.text, "html.parser")

            # Get all of the data rows as shown on the table
            for row in soup.find_all("tr"):
                if not isinstance(row, Tag):
                    continue

                data_id = row.get("data-id")

                # Ignore the data rows without `data-id` as it's just the template that
                # somehow they still sends, instead of just the rendered one?
                if data_id is None:
                    continue

                data_count += 1

                print(f"--ROW {data_count}-------------------------------------")

                columns = row.find_all("td")

                # Column one coincide with the data described below
                #
                # ```
                # Receiving
                # Committee Name
                # Committee ID-Type
                # ```
                #
                # In the real data, the Committee Name and ID-Type is separated by
                # a newline (an actual <br> on the source code)
                receiving_committee_name_id_and_type_raw = (
                    str(columns[0].text).strip().split("\n")
                )
                committee_name, commitee_id_and_type = (
                    receiving_committee_name_id_and_type_raw[0],
                    receiving_committee_name_id_and_type_raw[1],
                )
                # The second line of the column basically have format
                # of "Committee ID-Committee Type"
                committee_id, commitee_type = commitee_id_and_type.strip().split("-")

                # The second column coincide with the
                #
                # ```
                # Schedule Type
                # Description
                # ```
                #
                # Both of the data are actually on it's own span, thus we search
                # through span to get the data
                schedule_type_description_column_element = typing.cast(Tag, columns[1])
                schedule_type_description_span_elements = (
                    schedule_type_description_column_element.find_all("span")
                )
                # First span is the schedule type
                schedule_type_element = typing.cast(
                    Tag, schedule_type_description_span_elements[0]
                )

                # The second span is optional, but if available contains
                # the contribution description
                contribution_description = (
                    typing.cast(
                        Tag, schedule_type_description_span_elements[1]
                    ).text.strip()
                    if len(schedule_type_description_span_elements) > 1
                    else ""
                )
                schedule_type = schedule_type_element.text.strip()

                # Third column is the
                #
                # ```
                # Received From
                # Address
                # Occupation-Employer
                # ```
                #
                # While the "Received From" (contributor name) and "Occupation-Employer"
                # is just normal text, the "Address" is encapsulated within
                # <address> tag.
                received_from_address_occupation_employer_element = typing.cast(
                    Tag, columns[2]
                )

                print("Received From/Address/Occupation-Employer row:")
                print(received_from_address_occupation_employer_element)
                print("\n")

                adddress_element = (
                    received_from_address_occupation_employer_element.find("address"),
                )

                print("Address tag:")
                print(adddress_element)
                print(type(adddress_element))
                print("\n")

                if len(adddress_element) > 0 and isinstance(adddress_element[0], Tag):
                    adddress_element = adddress_element[0]

                    address_raw = (
                        adddress_element.text
                        if adddress_element.text is not None
                        else ""
                    ).strip()

                    # The address part on the tag is separated by a new line
                    # (real <br> element), though the within the line there could be
                    # whitespace before and after, thus this should clean the data
                    # up out of those extra whitespace
                    contributor_adddress = [
                        address_part.strip() for address_part in address_raw.split("\n")
                    ]
                    contributor_adddress = ", ".join(contributor_adddress)

                    # NOTE: this does looks "unclean", but overall this is the most
                    # reliable way to get text that's not part of any tags at
                    # all, by basically going back by several siblings
                    # to get the data
                    contributor_name = typing.cast(
                        Tag,
                        typing.cast(
                            Tag,
                            typing.cast(
                                Tag, adddress_element.previous_sibling
                            ).previous_sibling,
                        ).previous_sibling,
                    ).text.strip()
                    occupation_employer = (
                        str(
                            typing.cast(
                                Tag,
                                typing.cast(
                                    Tag, adddress_element.next_sibling
                                ).next_sibling,
                            ).text
                        )
                        .strip()
                        .split("-")
                    )
                else:
                    # If there's no <addreess> tag, it must be just "dues" type, and
                    # there should not be any address
                    contributor_adddress = ""

                    name_occupation_split = (
                        str(received_from_address_occupation_employer_element.text)
                        .strip()
                        .split("\n")
                    )

                    # There's possibility of no names as well
                    contributor_name, occupation_employer = (
                        (
                            name_occupation_split[0]
                            if len(name_occupation_split) > 0
                            else ""
                        ),
                        (
                            name_occupation_split[1]
                            if len(name_occupation_split) > 1
                            else ""
                        ),
                    )

                # There's not always an occupation employer data (espcially for PACs)
                contributor_occupation, contributor_employer = (
                    (
                        occupation_employer[0].strip()
                        if len(occupation_employer) > 0
                        else ""
                    ),
                    (
                        occupation_employer[1].strip()
                        if len(occupation_employer) > 1
                        else ""
                    ),
                )

                # Fourth column just contains the data with the format of
                # MM/DD/YYYY (classic American...)
                contribution_date = columns[3].text.strip()
                # Fifth column contains just the amount with dollar prefix of the amount
                contribution_amount = columns[4].text.strip()

                output_writer.writerow(
                    [
                        data_id,
                        committee_id,
                        commitee_type,
                        committee_name,
                        schedule_type,
                        contribution_description,
                        contributor_name,
                        contributor_adddress,
                        contributor_occupation,
                        contributor_employer,
                        contribution_date,
                        contribution_amount,
                    ]
                )

            context.log.info(
                f"Got {data_count} in page {page_number}"
                f" for date {context.partition_key}"
            )
            if data_count > 0:
                page_number += 1

                # Sleep to make sure we don't overwhelmed their server
                time.sleep(0.5 + random.uniform(0.5, 1))
            else:
                context.log.warning(
                    f"At page {page_number} got no data"
                    f" for date {context.partition_key}!"
                )
                break


@dg.asset(partitions_def=mi_daily_partition, deps=[mi_fetch_contributions])
def mi_insert_contributions(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert the current partition contributions data from MiTN
    """

    base_output_path = Path(MI_DATA_PATH_PREFIX)
    partition_result_dir = base_output_path / context.partition_key
    partition_result_path = partition_result_dir / "contributions.csv"

    current_partition_date = time.strptime(context.partition_key, "%Y-%m-%d")

    with pg.pool.connection() as pg_conn, pg_conn.cursor() as pg_cursor:
        context.log.info(
            f"Deleting contributions from {current_partition_date}"
            " to ensure no duplicates"
        )
        pg_cursor.execute(
            query=sql.SQL(
                "DELETE FROM mi_mitn_contributions_landing WHERE contribution_date = %s"
            ),
            params=(time.strftime("%m/%d/%Y", current_partition_date),),
        )

        current_partition_file_lines_generator = safe_readline_csv_like_file(
            file_path=partition_result_path
        )
        parsed_current_partition_file = csv.reader(
            current_partition_file_lines_generator,
        )

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_current_partition_file,
            table_name="mi_mitn_contributions_landing",
            table_columns_name=[
                "data_id",
                "committee_id",
                "commitee_type",
                "committee_name",
                "schedule_type",
                "contribution_description",
                "contributor_name",
                "contributor_adddress",
                "contributor_occupation",
                "contributor_employer",
                "contribution_date",
                "contribution_amount",
            ],
            row_validation_callback=lambda row: len(row) == 12,
        )


@dg.asset
def mi_fetch_legacy_contribution_files() -> dict[str, list[str]]:
    """
    Downloads Michigan campaign finance contribution data archive files.

    Downloads annual 7z legacy data archives from 2020 to 2025,
    extracts the contribution files, and saves them to the
    appropriate directory using pure Python libraries.

    Returns:
        A dictionary mapping years to lists of extracted file paths.
    """
    logger = dg.get_dagster_logger()

    # Create the Michigan data directory if it doesn't exist
    raw_dir = Path(MI_DATA_PATH_PREFIX, "raw")
    raw_dir.mkdir(exist_ok=True, parents=True)

    years_to_process = list(
        range(
            MI_LEGACY_FIRST_YEAR_AVAILABLE, MI_LEGACY_LAST_YEAR_AVAILABLE_INCLUSIVE + 1
        )
    )
    logger.info(f"Processing years: {years_to_process}")

    # Create archive file URLs for each year - using string year as key
    archive_urls = {
        str(year): f"{MI_LEGACY_CFR_BASE_URL}/{year}_mi_cfr.7z"
        for year in years_to_process
    }

    # Download and extract each archive file
    extracted_files = {}

    for year_str, archive_url in archive_urls.items():
        archive_path = raw_dir / f"{year_str}_mi_cfr.7z"
        year_dir = raw_dir / year_str
        year_dir.mkdir(exist_ok=True)

        if year_str not in extracted_files:
            extracted_files[year_str] = []

        # Download the archive if it doesn't exist
        if not archive_path.exists():
            logger.info(f"Downloading archive for {year_str}: {archive_url}")
            try:
                response = requests.get(
                    archive_url, headers=REQUEST_HEADERS, timeout=60
                )
                if response.status_code == 404:
                    logger.info(f"Archive for {year_str} not available yet, skipping")
                    continue
                elif response.status_code == 403:
                    logger.info(f"Access forbidden (403) for {archive_url}")
                    continue

                response.raise_for_status()

                # Save the downloaded 7z file
                with open(archive_path, "wb") as f:
                    f.write(response.content)

                logger.info(f"Successfully downloaded {archive_path}")

            except requests.RequestException as e:
                logger.error(f"Error downloading {archive_url}: {e}")
                continue
        else:
            logger.info(f"Archive already exists: {archive_path}")

        # Extract the archive using py7zr
        if not any(glob.glob(str(year_dir / "*.csv"))):
            logger.info(f"Extracting {archive_path} to {year_dir}")
            try:
                with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                    archive.extractall(path=year_dir)
                logger.info(f"Successfully extracted {archive_path}")

            except Exception as e:
                logger.error(f"Error extracting {archive_path}: {e}")
                continue
        else:
            logger.info(f"Files already extracted for {year_str}")

        # Get the list of extracted files
        for extracted_file in glob.glob(str(year_dir / "*.csv")):
            extracted_files[year_str].append(extracted_file)

    # Log the number of files extracted for each year
    for year_str, files in extracted_files.items():
        logger.info(f"Year {year_str}: found {len(files)} files")

    return extracted_files


@dg.asset(deps=[mi_fetch_legacy_contribution_files])
def mi_insert_legacy_contributions_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Processes the downloaded Michigan campaign contribution legacy
    files and loads them into the mi_contributions_landing table.

    This asset:
    1. Reads contribution data from all the extracted text files
    2. Truncates the landing table
    3. Inserts the data row by row
    4. Validates that each row has the correct number of columns

    Args:
        pg: A PostgreSQL connection resource

    Returns:
        A MaterializeResult with metadata about the insertion
    """
    logger = dg.get_dagster_logger()

    raw_dir = Path(MI_DATA_PATH_PREFIX, "raw")

    # Get all contribution files from all processed years - use string for year
    contribution_files = []
    for year in range(
        MI_LEGACY_FIRST_YEAR_AVAILABLE, MI_LEGACY_LAST_YEAR_AVAILABLE_INCLUSIVE + 1
    ):
        year_str = str(year)
        year_path = raw_dir / year_str
        if year_path.exists():
            for file in glob.glob(str(year_path / "*.csv")):
                # only if it has "contrib" in the name
                if "contrib" in file.lower():
                    contribution_files.append(file)

    logger.info(f"Found {len(contribution_files)} contribution files to process")

    # Truncate the landing table
    truncate_query = get_sql_truncate_query(table_name=MI_CONTRIBUTIONS_LANDING_TABLE)

    with (
        pg.pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {MI_CONTRIBUTIONS_LANDING_TABLE}")
        pg_cursor.execute(query=truncate_query)

        try:
            for file_path in contribution_files:
                file_size = os.path.getsize(file_path)

                if file_size == 0:
                    logger.warning(f"File {file_path} is empty, skipping...")
                    continue

                logger.info(f"Processing file: {file_path} ({file_size} bytes)")

                # Open and read the file line by line
                lines_generator = safe_readline_csv_like_file(
                    file_path,
                    encoding="utf-8",
                )

                # Parse the file using tab separator (per documentation)
                parsed_file = csv.reader(lines_generator, delimiter=",", quotechar='"')

                # Skip header row if present
                next(parsed_file, None)
                logger.info(f"Skipped header row in {file_path}")

                # Process the rest of the file
                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_file,
                    table_name=MI_CONTRIBUTIONS_LANDING_TABLE,
                    table_columns_name=MI_CONTRIBUTIONS_COLUMNS,
                    row_validation_callback=lambda row: len(row)
                    == len(MI_CONTRIBUTIONS_COLUMNS),
                )

        except Exception as e:
            logger.error(f"Error while processing contribution files: {e}")
            raise e

        # Count the number of rows inserted
        count_query = get_sql_count_query(table_name=MI_CONTRIBUTIONS_LANDING_TABLE)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        row_count = int(count_cursor_result[0]) if count_cursor_result else 0

        logger.info(f"Inserted {row_count} rows into {MI_CONTRIBUTIONS_LANDING_TABLE}")

        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": MI_CONTRIBUTIONS_LANDING_TABLE,
                "dagster/row_count": row_count,
                "timestamp": datetime.now().isoformat(),
            }
        )

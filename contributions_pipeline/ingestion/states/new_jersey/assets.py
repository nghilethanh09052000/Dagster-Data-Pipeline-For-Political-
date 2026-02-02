import csv
import os
import shutil
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

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

# URLs for the New Jersey contribution data files
NJ_GUBERNATORIAL_URL = (
    "https://www.elec.nj.gov/download/Data/Gubernatorial/All_GUB_Text.zip"
)
NJ_LEGISLATIVE_URL = (
    "https://www.elec.nj.gov/download/Data/Legislative/All_LEG_Text.zip"
)
NJ_COUNTYWIDE_URL = "https://www.elec.nj.gov/download/Data/Countywide/All_CW_Text.zip"
NJ_PAC_URL = "https://www.elec.nj.gov/download/Data/PAC/All_PAC_Text.zip"

# Data path prefix for storing the downloaded and extracted files
NJ_DATA_PATH_PREFIX = "./states/new_jersey"

# New Jersey Candidate Configuration
NJ_CANDIDATE_API_URL = "https://www.njelecefilesearch.com/api/VWEntity/Entities20"
NJ_CANDIDATE_DATA_PATH = "./states/new_jersey/candidates"
NJ_CANDIDATE_START_DATE = datetime(1981, 1, 1)

# Candidate table columns
NJ_CANDIDATE_COLUMNS = [
    "entity_s",
    "entity_name",
    "office",
    "party",
    "location",
    "election_year",
    "election_type",
    "tot_cont_amt",
    "tot_exp_amt",
]

# Column names for the NJ contributions files
# These match the nj_contributions_landing table structure
NJ_CONTRIBUTION_COLUMNS = [
    "cont_lname",
    "cont_fname",
    "cont_mname",
    "cont_suffix",
    "cont_non_ind_name",
    "cont_non_ind_name2",
    "cont_street1",
    "cont_street2",
    "cont_city",
    "cont_state",
    "cont_zip",
    "cont_type",
    "cont_amt",
    "receipt_type",
    "cont_date",
    "occupation",
    "emp_name",
    "emp_street1",
    "emp_street2",
    "emp_city",
    "emp_state",
    "emp_zip",
    "rec_lname",
    "rec_fname",
    "rec_mname",
    "rec_suffix",
    "rec_non_ind_name",
    "rec_non_ind_name2",
    "office",
    "party",
    "location",
    "election_year",
    "election_type",
    "src_file",
]


def _guess_delimiter(file_path: str) -> str:
    """
    Determine if a file is tab-delimited or comma-delimited.

    NJ files can be in two formats (tab or comma delimited)
    as mentioned in the documentation.
    """
    with open(file_path, errors="replace") as f:
        first_line = f.readline()
        if first_line.count("\t") > first_line.count(","):
            return "\t"
        return ","


def nj_insert_contributions_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str = "nj_contributions_landing",
) -> dg.MaterializeResult:
    """
    Insert all extracted New Jersey contribution files to the landing table.

    This processes all the text files extracted from the four ZIP files and loads
    them into the landing table.
    """
    logger = dg.get_dagster_logger(name="nj_contributions_insert")

    # Directory where all extracted files are stored
    data_dir = Path(NJ_DATA_PATH_PREFIX)

    # Get all text files in the directory
    txt_files = list(data_dir.glob("*.txt"))

    # Truncate the landing table before inserting new data
    truncate_query = get_sql_truncate_query(table_name=table_name)

    total_rows = 0

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting new data")
        pg_cursor.execute(query=truncate_query)

        # Process each text file
        for txt_file in txt_files:
            file_path = str(txt_file)
            file_name = txt_file.name

            try:
                # Determine delimiter for this file
                delimiter = _guess_delimiter(file_path)
                logger.info(f"Processing file {file_name} with delimiter '{delimiter}'")

                # Read and parse the file
                file_lines_generator = safe_readline_csv_like_file(file_path)

                # Skip header row
                next(file_lines_generator, None)

                # Parse the file with the determined delimiter
                parsed_file = csv.reader(
                    file_lines_generator,
                    delimiter=delimiter,
                    quotechar='"',
                )

                # Define validation callback to make sure rows are consistent
                def validate_row(row: list[str], file_name=file_name) -> bool:
                    # Add the source file name to each row
                    row.append(file_name)
                    # Check if the row has the right number of columns
                    return len(row) == len(NJ_CONTRIBUTION_COLUMNS)

                # Insert the data into the landing table
                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_file,
                    table_name=table_name,
                    table_columns_name=NJ_CONTRIBUTION_COLUMNS,
                    row_validation_callback=validate_row,
                )

            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                raise e

        # Count total rows inserted
        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        total_rows = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Inserted {total_rows} rows into {table_name}")

    return dg.MaterializeResult(
        metadata={"dagster/table_name": table_name, "dagster/row_count": total_rows}
    )


@dg.asset()
def nj_fetch_contribution_data(context: dg.AssetExecutionContext):
    """
    Fetch the New Jersey contribution data files and extract them.

    Downloads all four ZIP files (Gubernatorial, Legislative, Countywide, and PAC)
    and extracts the contained text files.
    """
    # Create the data directory if it doesn't exist
    Path(NJ_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)

    # URLs to download
    url_map = {
        "gubernatorial": NJ_GUBERNATORIAL_URL,
        "legislative": NJ_LEGISLATIVE_URL,
        "countywide": NJ_COUNTYWIDE_URL,
        "pac": NJ_PAC_URL,
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/138.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.elec.nj.gov/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    for category, url in url_map.items():
        zip_file_path = f"{NJ_DATA_PATH_PREFIX}/{category}.zip"

        context.log.info(f"Downloading {category} data from {url}")

        # Download the ZIP file
        stream_download_file_to_path(
            request_url=url, file_save_path=zip_file_path, headers=headers
        )

        try:
            # Extract all files from the ZIP
            with ZipFile(zip_file_path, "r") as zip_ref:
                context.log.info(f"Extracting {category} files")

                for file_info in zip_ref.infolist():
                    # Skip directories
                    if file_info.filename.endswith("/"):
                        continue

                    extract_path = f"{NJ_DATA_PATH_PREFIX}/{file_info.filename}"

                    # Ensure parent directories exist
                    Path(extract_path).parent.mkdir(parents=True, exist_ok=True)
                    # Extract file using buffered streaming
                    with (
                        zip_ref.open(file_info) as source,
                        open(extract_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)  # 64KB buffer
                        context.log.info(f"Extracted {file_info.filename}")
        finally:
            # Clean up the ZIP file
            if os.path.exists(zip_file_path):
                os.remove(zip_file_path)
                context.log.info(f"Removed ZIP file {zip_file_path}")


@dg.asset(deps=[nj_fetch_contribution_data], pool="pg")
def nj_contributions_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all New Jersey contribution data into the landing table.

    This asset depends on the fetch operation and will process all text files
    to load them into the nj_contributions_landing table.
    """
    return nj_insert_contributions_to_landing_table(
        postgres_pool=pg.pool,
        table_name="nj_contributions_landing",
    )


# Candidate yearly partition definition
nj_candidate_yearly_partition = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
    fmt="%Y",
    start=NJ_CANDIDATE_START_DATE,
    end_offset=1,
)


@dg.asset(partitions_def=nj_candidate_yearly_partition)
def nj_fetch_candidates_by_year(context: dg.AssetExecutionContext) -> dict:
    """
    Fetch candidate data from New Jersey election file search API
    for a specific year.

    This asset is partitioned by year, starting from 1981 to current year.
    """
    # Get the year from partition key
    year = context.partition_key
    context.log.info(f"Fetching candidates for year: {year}")

    # Create the data directory if it doesn't exist
    year_dir = Path(NJ_CANDIDATE_DATA_PATH) / year
    year_dir.mkdir(parents=True, exist_ok=True)

    # API request headers
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.njelecefilesearch.com",
        "Referer": "https://www.njelecefilesearch.com/SearchEntityList",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Ch-Ua": (
            '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"'
        ),
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    # Election type codes to loop through
    election_type_codes = ["F", "G", "I", "X", "M", "P", "D", "R", "B", "E", "S"]
    election_type_names = {
        "F": "FIRE COMMISSIONER",
        "G": "GENERAL",
        "I": "INAUGURAL",
        "X": "LEGAL",
        "M": "MUNICIPAL - MAY",
        "P": "PRIMARY",
        "D": "RUN-OFF FOR GENERAL ELECTION",
        "R": "RUNOFF",
        "B": "SCHOOL BOARD-APRIL",
        "E": "SCHOOL BOARD-NOV.",
        "S": "SPECIAL (INCLUDES RECALL)",
    }

    all_candidates = []
    total_records = 0
    total_filtered = 0

    # Loop through each election type
    for election_type_code in election_type_codes:
        election_type_name = election_type_names.get(election_type_code, "")
        context.log.info(
            f"Processing election type:\
            {election_type_code} ({election_type_name})"
        )

        draw = 1
        start = 0
        page_size = 1000

        while True:
            # API request data (form parameters)
            data = {
                "draw": str(draw),
                "columns[0][data]": "ENTITYNAME",
                "columns[0][name]": "ENTITYNAME",
                "columns[0][searchable]": "true",
                "columns[0][orderable]": "true",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
                "columns[1][data]": "LOCATION",
                "columns[1][name]": "LOCATION",
                "columns[1][searchable]": "true",
                "columns[1][orderable]": "true",
                "columns[1][search][value]": "",
                "columns[1][search][regex]": "false",
                "columns[2][data]": "OFFICE",
                "columns[2][name]": "OFFICE",
                "columns[2][searchable]": "true",
                "columns[2][orderable]": "true",
                "columns[2][search][value]": "",
                "columns[2][search][regex]": "false",
                "columns[3][data]": "PARTY",
                "columns[3][name]": "PARTY",
                "columns[3][searchable]": "true",
                "columns[3][orderable]": "true",
                "columns[3][search][value]": "",
                "columns[3][search][regex]": "false",
                "columns[4][data]": "ELECTIONTYPE",
                "columns[4][name]": "ELECTIONTYPE",
                "columns[4][searchable]": "true",
                "columns[4][orderable]": "true",
                "columns[4][search][value]": "",
                "columns[4][search][regex]": "false",
                "columns[5][data]": "ELECTIONYEAR",
                "columns[5][name]": "ELECTIONYEAR",
                "columns[5][searchable]": "true",
                "columns[5][orderable]": "true",
                "columns[5][search][value]": "",
                "columns[5][search][regex]": "false",
                "order[0][column]": "5",
                "order[0][dir]": "desc",
                "order[0][name]": "ELECTIONYEAR",
                "order[1][column]": "0",
                "order[1][dir]": "asc",
                "order[1][name]": "ENTITYNAME",
                "start": str(start),
                "length": str(page_size),
                "search[value]": "",
                "search[regex]": "false",
                "NONPACOnly": "true",
                "OfficeCodes": "",
                "PartyCodes": "",
                "LocationCodes": "",
                "ElectionTypeCodes": election_type_code,
                "ElectionYears": year,
                "SortColumn": "ElectionYear",
                "SortBy": "desc",
            }

            context.log.info(
                f"Making POST request for year {year}, \
                election type {election_type_code}, "
                f"draw {draw}, start {start}"
            )

            # Make the POST request
            response = requests.post(
                url=NJ_CANDIDATE_API_URL, headers=headers, data=data, timeout=30
            )

            # Check if request was successful
            response.raise_for_status()

            # Parse JSON response
            api_data = response.json()

            if not api_data.get("data"):
                context.log.info(
                    f"No more data for year {year}, election type {election_type_code}"
                )
                break

            # Extract candidate data
            page_candidates = []
            for record in api_data["data"]:
                candidate = {
                    "entity_s": record.get("ENTITY_S", ""),
                    "entity_name": record.get("ENTITYNAME", ""),
                    "office": record.get("OFFICE", ""),
                    "party": record.get("PARTY", ""),
                    "location": record.get("LOCATION", ""),
                    "election_year": record.get("ELECTIONYEAR", ""),
                    "election_type": record.get("ELECTIONTYPE", ""),
                    "tot_cont_amt": record.get("TOT_CONT_AMT", ""),
                    "tot_exp_amt": record.get("TOT_EXP_AMT", ""),
                }
                page_candidates.append(candidate)

            all_candidates.extend(page_candidates)

            context.log.info(
                f"Received {len(page_candidates)} records for year {year}, "
                f"election type {election_type_code}, draw {draw}"
            )

            # Check if we've reached the end
            if len(page_candidates) < page_size:
                context.log.info(
                    f"Reached end of data for year {year}, \
                    election type {election_type_code}"
                )
                break

            # Move to next page
            draw += 1
            start += page_size

        # Update totals for this election type
        total_records += api_data.get("recordsTotal", 0)
        total_filtered += api_data.get("recordsFiltered", 0)

    # Save all candidates to CSV file
    csv_file = year_dir / f"candidates_{year}.csv"
    if all_candidates:
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NJ_CANDIDATE_COLUMNS)
            writer.writeheader()
            writer.writerows(all_candidates)
        context.log.info(
            f"Total candidate data saved to {csv_file}: {len(all_candidates)} records"
        )
    else:
        context.log.warning(f"No candidate data found for year {year}")

    return {
        "year": year,
        "total_records": len(all_candidates),
        "csv_file": str(csv_file) if all_candidates else None,
        "records_total": total_records,
        "records_filtered": total_filtered,
    }


@dg.asset(
    deps=[nj_fetch_candidates_by_year],
    pool="pg",
    partitions_def=nj_candidate_yearly_partition,
)
def nj_candidates_insert_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert candidate data into the landing table.

    This asset depends on the fetch operation and will process all CSV files
    to load them into the nj_candidates_landing table.
    """
    logger = dg.get_dagster_logger(name="nj_candidates_insert_to_landing_table")
    year = int(context.partition_key)

    # Get all CSV files from the candidate data directory for this year
    year_dir = Path(NJ_CANDIDATE_DATA_PATH) / str(year)
    csv_files = list(year_dir.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found for year {year}")
        return dg.MaterializeResult(metadata={"dagster/row_count": 0})

    table_name = "nj_candidates_landing"
    truncate_query = sql.SQL("DELETE FROM {} WHERE election_year = {}").format(
        sql.Identifier(table_name), sql.Literal(str(year))
    )

    total_rows = 0

    with (
        pg.pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting new data")
        pg_cursor.execute(query=truncate_query)

        # Process each CSV file
        for csv_file in csv_files:
            file_path = str(csv_file)
            file_name = csv_file.name

            try:
                logger.info(f"Processing file {file_name}")

                # Read and parse the file
                file_lines_generator = safe_readline_csv_like_file(file_path)

                # Skip header row
                next(file_lines_generator, None)

                # Parse the file with comma delimiter
                parsed_file = csv.reader(
                    file_lines_generator,
                    delimiter=",",
                    quotechar='"',
                )

                # Insert the data into the landing table
                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_file,
                    table_name=table_name,
                    table_columns_name=NJ_CANDIDATE_COLUMNS,
                    row_validation_callback=(
                        lambda row: len(row) == len(NJ_CANDIDATE_COLUMNS)
                    ),
                )

            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                raise e

        # Count total rows inserted
        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        total_rows = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Inserted {total_rows} rows into {table_name}")

    return dg.MaterializeResult(
        metadata={"dagster/table_name": table_name, "dagster/row_count": total_rows}
    )


# Define a list containing all NJ assets
nj_assets = [
    nj_fetch_contribution_data,
    nj_contributions_insert_to_landing_table,
    nj_fetch_candidates_by_year,
    nj_candidates_insert_to_landing_table,
]

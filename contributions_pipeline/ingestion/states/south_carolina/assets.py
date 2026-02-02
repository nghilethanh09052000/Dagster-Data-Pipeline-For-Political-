import csv
import json
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Constants for South Carolina contribution data
SC_API_URL = "https://ethicsfiling.sc.gov/api/Candidate/Contribution/Search/"
SC_DATA_PATH_PREFIX = "./states/south_carolina"

# Column names for the SC contributions landing table - based on the documentation
SC_CONTRIBUTION_COLUMNS = [
    "contribution_id",
    "office_run_id",
    "candidate_id",
    "date",
    "amount",
    "candidate_name",
    "office_name",
    "election_date",
    "contributor_name",
    "contributor_occupation",
    "group",
    "contributor_address",
    "description",
]


def sc_fetch_contribution_data(context: dg.AssetExecutionContext):
    """
    Fetch South Carolina campaign contribution data from the SC Ethics Commission API.

    This function makes a POST request to the SC Ethics Commission API to download
    all contributions since 2000 and saves the results as a JSON file.

    Based on the R diary documentation at:
    /pipeline/contributions_pipeline/ingestion/states/south_carolina/sc_contribs_diary.md
    """
    # Create the data directory if it doesn't exist
    Path(SC_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)
    json_file_path = f"{SC_DATA_PATH_PREFIX}/sc_contributions.json"

    context.log.info(f"Fetching South Carolina contribution data from {SC_API_URL}")

    # Prepare the POST request body
    # The request parameters are based directly on the R documentation
    request_body = {
        "amountMax": 0,
        "amountMin": 0,
        "candidate": "",
        "contributionDateMax": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "contributionDateMin": "2000-01-01T05:00:00.000Z",
        "contributionDescription": "",
        "contributorCity": "",
        "contributorName": "",
        "contributorOccupation": "",
        "contributorZip": None,
        "officeRun": "",
    }

    # Make POST request and save the response to file
    try:
        context.log.info("Making POST request to SC Ethics Commission API")
        response = requests.post(
            url=SC_API_URL,
            json=request_body,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"fetch failed: {response.status_code} - {response.text}"
            )

        # Write the response JSON to a file
        with open(json_file_path, "w") as f:
            f.write(response.text)

        context.log.info(f"South Carolina contribution data saved to {json_file_path}")
        return json_file_path
    except Exception as e:
        context.log.error(f"Error fetching South Carolina contribution data: {e}")
        raise e


def sc_convert_json_to_csv(
    context: dg.AssetExecutionContext, json_file_path: str | None = None
):
    """
    Convert the raw JSON response to CSV format for easier processing.

    This function reads the JSON data and converts it to a CSV file with the
    proper structure for the landing table.
    """
    if json_file_path is None:
        json_file_path = f"{SC_DATA_PATH_PREFIX}/sc_contributions.json"

    csv_file_path = f"{SC_DATA_PATH_PREFIX}/sc_contributions.csv"

    context.log.info("Converting JSON data to CSV format")

    try:
        # Read the JSON file
        with open(json_file_path) as f:
            contributions = json.load(f)

        # Write to CSV
        with open(csv_file_path, "w", newline="") as f:
            writer = csv.writer(
                f,
                quoting=csv.QUOTE_MINIMAL,
                delimiter=",",
                quotechar='"',
                escapechar="\\",
            )
            # Write header
            writer.writerow(SC_CONTRIBUTION_COLUMNS)

            # Write data rows
            for contrib in contributions:
                # Convert boolean group to string "Yes" or "No" as per the documentation
                group_value = "Yes" if contrib.get("group", False) else "No"

                row = [
                    contrib.get("contributionId", ""),
                    contrib.get("officeRunId", ""),
                    contrib.get("candidateId", ""),
                    contrib.get("date", ""),
                    contrib.get("amount", ""),
                    contrib.get("candidateName", ""),
                    contrib.get("officeName", ""),
                    contrib.get("electionDate", ""),
                    contrib.get("contributorName", ""),
                    contrib.get("contributorOccupation", ""),
                    group_value,
                    contrib.get("contributorAddress", ""),
                    contrib.get("description", ""),
                ]
                writer.writerow(row)

        context.log.info(
            f"South Carolina contribution data converted to CSV at {csv_file_path}"
        )
        return csv_file_path
    except Exception as e:
        context.log.error(
            f"Error converting South Carolina contribution data to CSV: {e}"
        )
        raise e


def sc_insert_contributions_to_landing_table(
    postgres_pool: ConnectionPool,
    table_name: str = "sc_contributions_landing",
    data_file: str = f"{SC_DATA_PATH_PREFIX}/sc_contributions.csv",
) -> dg.MaterializeResult:
    """
    Insert South Carolina contribution data to landing table in Postgres.

    This function reads the CSV file and loads it into the landing table.
    Based on the R diary documentation, we're loading raw data directly into
    the landing table without significant transformation.
    """
    logger = dg.get_dagster_logger(name="sc_contributions_insert")

    # Truncate the landing table before inserting new data
    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name} before inserting new data.")
        pg_cursor.execute(truncate_query)

        logger.info(f"Processing file: {data_file}")
        file_lines_generator = safe_readline_csv_like_file(data_file, encoding="utf-8")

        # Read as comma-separated values
        parsed_file = csv.reader(
            file_lines_generator, delimiter=",", quotechar='"', escapechar="\\"
        )

        # Skip the header
        next(parsed_file, None)

        # Define validation function to ensure correct number of columns
        def validate_row(row: list[str]) -> bool:
            return len(row) == len(SC_CONTRIBUTION_COLUMNS)

        # Insert the data into the landing table
        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_file,
            table_name=table_name,
            table_columns_name=SC_CONTRIBUTION_COLUMNS,
            row_validation_callback=validate_row,
        )

        # Verify row count
        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Successfully inserted {row_count} rows into {table_name}")
        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


# Define Dagster assets
@dg.asset()
def sc_fetch_raw_data(context: dg.AssetExecutionContext):
    """
    Fetch South Carolina contribution data from the Ethics Commission API.
    """
    return sc_fetch_contribution_data(context)


@dg.asset(deps=[sc_fetch_raw_data])
def sc_process_data(context: dg.AssetExecutionContext, sc_fetch_raw_data):
    """
    Process the raw JSON data and convert it to CSV format.
    """
    return sc_convert_json_to_csv(context, sc_fetch_raw_data)


@dg.asset(deps=[sc_process_data], pool="pg")
def sc_contributions_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource], sc_process_data
):
    """
    Insert South Carolina contribution data into the landing table.
    """
    return sc_insert_contributions_to_landing_table(
        postgres_pool=pg.pool,
        table_name="sc_contributions_landing",
        data_file=sc_process_data,
    )


# Define a list containing all SC assets
sc_assets = [
    sc_fetch_raw_data,
    sc_process_data,
    sc_contributions_insert_to_landing_table,
]

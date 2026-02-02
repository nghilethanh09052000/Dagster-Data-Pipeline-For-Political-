import json
import time
from pathlib import Path

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.ingest import (
    COPY_INSERT_BATCH_NUM,
    get_sql_binary_format_copy_query_for_landing_table,
    get_sql_truncate_query,
)
from contributions_pipeline.lib.iter import batched
from contributions_pipeline.resources import PostgresResource

MS_DATA_BASE_PATH = "./states/mississippi"

MS_DATA_CONTRIBUTIONS_FILE_NAME = "contributions.json"
MS_DATA_EXPENDITURES_FILE_NAME = "expenditures.json"
MS_DATA_CANDIDATE_COMMITTEE_FILE_NAME = "candidate_committee.json"

MS_API_BASE_URL = (
    "https://cfportal.sos.ms.gov/online/Services/MS/CampaignFinanceServices.asmx/"
)


def ms_stream_raw_report_from_api(action_name: str, payload: object, file_path: Path):
    """
    Stream JSON file from mississippi API to a specified file path.

    Params:
    action_name: name of the query action you want to do on mississippi API.
                 For example to query contributions, input "ContributionSearch".
                 Basically you should put whatever after
                 https://cfportal...Services.asmx/<This Part>
    payload: whatever payload the action requires. This should be a valid object
             as it will be sent as json
    file_path: the path where the file will be written
    """
    logger = dg.get_dagster_logger("ms_stream_report")

    logger.info(f"Starting to stream contributions request for action {action_name}")
    with (
        requests.session() as req_session,
        req_session.post(
            MS_API_BASE_URL + action_name,
            json=payload,
            stream=True,
        ) as response,
    ):
        if not response.ok:
            logger.warning(f"Header: {response.headers}")
            logger.warning(f"Body: {response.text}")

            response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def ms_insert_raw_json_to_landing(
    postgres_pool: ConnectionPool,
    raw_file_path: Path,
    table_json_keys: list[str],
    table_name: str,
    table_columns: list[str],
):
    """
    Insert raw JSON file from mississippi API to a landing table.

    This function assume that all of the API response from mississippi basially follows
    the following pattern.

    ```
    "d" -> string of quoted json
           -> "Table"
              -> Array of object -> Items
    ```

    Params:
    connection_pool: postgres connection pool, could use the resources already
                     configured for postgres
    raw_file_path: location of the raw JSON file
    table_json_keys: list of keys of items on the array `d.(parsed).Table.[]`
    table_name: the name of the landing table you want the data to be inserted into
    table_columns: list of the table columns. You need to make sure that the table
                   columns follows the sorting of table_json_keys. As it will be
                   one to one mapping from that key to the table columns by index.
    """
    logger = dg.get_dagster_logger("ms_insert_raw_json")

    # Encoding must be utf8 as Postgres copy only seems to supoort that one,
    # and definitely not us-ascii which is the file actual encoding
    with open(
        raw_file_path, encoding="utf8", errors="ignore"
    ) as contributions_raw_file:
        raw_parsed_file = json.load(contributions_raw_file)
        raw_parsed_data = json.loads(raw_parsed_file["d"])

        parsed_tables_file = raw_parsed_data["Table"]

    truncate_query = get_sql_truncate_query(table_name=table_name)
    copy_query = get_sql_binary_format_copy_query_for_landing_table(
        table_name=table_name,
        table_columns_name=table_columns,
    )

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for row_batch in batched(iterable=parsed_tables_file, n=COPY_INSERT_BATCH_NUM):
            chunk_len = len(row_batch)
            start_time = time.time()

            with pg_cursor.copy(statement=copy_query) as copy_cursor:
                for row_data in row_batch:
                    cleaned_row = []
                    for key in table_json_keys:
                        value = row_data[key]
                        # Cleanup stray NUL characters not supported by postgres
                        cleaned_value = (
                            value.replace("\x00", "")
                            if isinstance(value, str)
                            else value
                        )

                        cleaned_row.append(cleaned_value)

                    copy_cursor.write_row(cleaned_row)

            elapsed = time.time() - start_time

            logger.info(f"batch done: {round(chunk_len / elapsed, 3)} rows/s")


@dg.asset()
def ms_contributions():
    """
    Retrieve all mississippi contributions data from their API.
    Data from the API are available from 2016 - now.

    This asset will stream data from mississippi cfportal in JSON format to
    `states/mississippi/contributions.json`.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    file_save_path = base_path / MS_DATA_CONTRIBUTIONS_FILE_NAME

    ms_stream_raw_report_from_api(
        action_name="ContributionSearch",
        payload={
            "EntityName": "",
            "Description": "",
            "BeginDate": "",
            "EndDate": "",
            "AmountPaid": "",
            "InKindAmount": "",
            "CandidateName": "",
            "CommitteeName": "",
            "ContributionType": "Any",
        },
        file_path=file_save_path,
    )


@dg.asset()
def ms_expenditures():
    """
    Retrieve all mississippi expenditures data from their API.
    Data from the API are available from 2016-10-01 - now.

    This asset will stream data from mississippi cfportal in JSON format to
    `states/mississippi/expenditures.json`.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    file_save_path = base_path / MS_DATA_EXPENDITURES_FILE_NAME

    ms_stream_raw_report_from_api(
        action_name="ExpenditureSearch",
        payload={
            "EntityName": "",
            "Description": "",
            "BeginDate": "",
            "EndDate": "",
            "AmountPaid": "",
            "CandidateName": "",
            "CommitteeName": "",
        },
        file_path=file_save_path,
    )


@dg.asset()
def ms_candidate_committee():
    """
    Retrieve all mississippi candidates/committee data from their API.

    This asset will stream data from mississippi cfportal in JSON format to
    `states/mississippi/expenditures.json`.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    file_save_path = base_path / MS_DATA_CANDIDATE_COMMITTEE_FILE_NAME

    ms_stream_raw_report_from_api(
        action_name="CandidateNameSearch",
        payload={"SearchBy": "Contains", "EntityName": "", "SearchType": "All"},
        file_path=file_save_path,
    )


@dg.asset(deps=[ms_contributions])
def ms_insert_contributions(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all mississippi contributions data to landing table.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    file_save_path = base_path / MS_DATA_CONTRIBUTIONS_FILE_NAME

    ms_insert_raw_json_to_landing(
        postgres_pool=pg.pool,
        raw_file_path=file_save_path,
        table_json_keys=[
            "Recipient",
            "ReferenceNumber",
            "FilingDesc",
            "FilingId",
            "Contributor",
            "ContributorType",
            "AddressLine1",
            "City",
            "StateCode",
            "PostalCode",
            "InKind",
            "Occupation",
            "Date",
            "Amount",
        ],
        table_name="ms_contributions_landing",
        table_columns=[
            "Recipient",
            "ReferenceNumber",
            "FilingDesc",
            "FilingId",
            "Contributor",
            "ContributorType",
            "AddressLine1",
            "City",
            "StateCode",
            "PostalCode",
            "InKind",
            "Occupation",
            "Date",
            "Amount",
        ],
    )


@dg.asset(deps=[ms_expenditures])
def ms_insert_expenditures(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all mississippi expenditures data to landing table.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    file_save_path = base_path / MS_DATA_EXPENDITURES_FILE_NAME

    ms_insert_raw_json_to_landing(
        postgres_pool=pg.pool,
        raw_file_path=file_save_path,
        table_json_keys=[
            "Filer",
            "ReferenceNumber",
            "FilingDesc",
            "FilingId",
            "Recipient",
            "AddressLine1",
            "City",
            "StateCode",
            "PostalCode",
            "Description",
            "Date",
            "Amount",
        ],
        table_name="ms_expenditures_landing",
        table_columns=[
            "Filer",
            "ReferenceNumber",
            "FilingDesc",
            "FilingId",
            "Recipient",
            "AddressLine1",
            "City",
            "StateCode",
            "PostalCode",
            "Description",
            "Date",
            "Amount",
        ],
    )


@dg.asset(deps=[ms_candidate_committee])
def ms_insert_candidate_committee(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all mississippi candidate/committee data to landing table.
    """
    base_path = Path(MS_DATA_BASE_PATH)
    file_save_path = base_path / MS_DATA_CANDIDATE_COMMITTEE_FILE_NAME

    ms_insert_raw_json_to_landing(
        postgres_pool=pg.pool,
        raw_file_path=file_save_path,
        table_json_keys=[
            "EntityId",
            "EntityName",
            "OrganizationType",
        ],
        table_name="ms_candidate_committee_landing",
        table_columns=[
            "EntityId",
            "EntityName",
            "OrganizationType",
        ],
    )

import csv
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

DC_REPORT_TYPE = {
    "CONTRIB": {"id": "84d0702e81954f918551c2f7c4355382", "layer": 34},
    "EXPENDITURES": {"id": "5638336c30ea4c69805ebb838f22794a", "layer": 35},
}
DC_DATA_PATH = "./states/district_of_columbia"

# URLs for committee data
DC_COMMITTEE_BASE_URL = "https://efiling.ocf.dc.gov"
DC_COMMITTEE_SEARCH_URL = f"{DC_COMMITTEE_BASE_URL}/ActiveCandidates"
DC_COMMITTEE_SUBMIT_URL = f"{DC_COMMITTEE_BASE_URL}/ActiveCandidates/SubmitSearch"
DC_COMMITTEE_DATA_URL = f"{DC_COMMITTEE_BASE_URL}/ActiveCandidates/Search"

# Headers for requests
DC_COMMITTEE_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
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
        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
}


def get_url_for_dc_bulk_download_one_category(**url_param) -> str:
    """
    Get district of columbia bulk download URL
    for specific prefix (report type) for a cycle
    """

    return f"https://opendata.dc.gov/api/download/v1/items/{url_param['id']}/csv?layers={url_param['layer']}"


def dc_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    data_file_path: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Insert all available data files from one report type to its repsective postgres
    landing table.

    This function assumed that all of the files have the same columns.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.
    data_file_path: the file path of the actual data file on the year folder, for
                    example contributions, the local folder for it is laid out as it
                    follows (year) -> contrib.txt. The data file is itcont.txt,
                    thus the input should be "/contrib.txt".
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted.
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    """

    logger = dg.get_dagster_logger(f"dc_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(DC_DATA_PATH)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        current_cycle_data_file_path = base_path / f"{data_file_path}"

        logger.info(f"Inserting file to pg ({current_cycle_data_file_path})")

        try:
            current_cycle_file_lines_generator = safe_readline_csv_like_file(
                file_path=current_cycle_data_file_path, encoding="utf-8"
            )
            parsed_current_cycle_file = csv.reader(
                current_cycle_file_lines_generator,
                delimiter=",",
                quotechar='"',
            )

            next(parsed_current_cycle_file)

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_current_cycle_file,
                table_name=table_name,
                table_columns_name=table_columns_name,
                row_validation_callback=data_validation_callback,
            )

        except FileNotFoundError:
            logger.warning(f"File for {data_file_path} is non-existent, ignoring...")
        except Exception as e:
            logger.error(f"Got error while reading {data_file_path} file: {e}")
            raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset()
def dc_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all dc finance campaign report.
    """

    file_path = Path(DC_DATA_PATH)
    file_path.mkdir(parents=True, exist_ok=True)

    for report_type, url_params in DC_REPORT_TYPE.items():
        context.log.info(f"Downloading campaign full export data for {report_type}")

        url = get_url_for_dc_bulk_download_one_category(**url_params)

        try:
            stream_download_file_to_path(
                request_url=url, file_save_path=file_path / f"{report_type}.csv"
            )

        except FileNotFoundError:
            context.log.info(f"File for {report_type} is non-existent, ignoring... ")
        except Exception as e:
            context.log.info(f"Got error while reading {report_type} file: {e}")
            raise e


@dg.asset(deps=[dc_fetch_all_campaign_data_full_export])
def dc_insert_financial_contrib_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format DC CONTRIB.csv data to the right landing table"""

    dc_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="CONTRIB.csv",
        table_name="dc_financial_contributions_landing_table",
        table_columns_name=[
            "COMMITTEENAME",
            "CANDIDATENAME",
            "ELECTIONYEAR",
            "CONTRIBUTORNAME",
            "CONTRIBUTORTYPE",
            "CONTRIBUTIONTYPE",
            "ADDRESS",
            "FULLADDRESS",
            "WARD",
            "EMPLOYER",
            "EMPLOYERADDRESS",
            "AMOUNT",
            "DATEOFRECEIPT",
            "ADDRESS_ID",
            "XCOORD",
            "YCOORD",
            "LATITUDE",
            "LONGITUDE",
            "GIS_LAST_MOD_DTTM",
            "OBJECTID",
        ],
        data_validation_callback=lambda row: len(row) == 20,
    )


@dg.asset(deps=[dc_fetch_all_campaign_data_full_export])
def dc_insert_financial_expenditures_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format DC EXPENDITURES.csv data to the right landing table"""

    dc_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="EXPENDITURES.csv",
        table_name="dc_financial_expenditures_landing_table",
        table_columns_name=[
            "OBJECTID",
            "CANDIDATENAME",
            "PAYEE",
            "ADDRESS",
            "PURPOSE",
            "AMOUNT",
            "TRANSACTIONDATE",
            "ADDRESS_ID",
            "XCOORD",
            "YCOORD",
            "LATITUDE",
            "LONGITUDE",
            "FULLADDRESS",
            "GIS_LAST_MOD_DTTM",
            "WARD",
        ],
        data_validation_callback=lambda row: len(row) == 15,
    )


@dg.asset
def dc_fetch_committees_data() -> None:
    """Fetches committee data from DC OCF website.

    This asset performs the following steps:
    1. Makes initial request to get session cookies
    2. Submits search form to initialize search
    3. Exports committee data to CSV
    4. Saves results to file
    """
    logger = dg.get_dagster_logger(name="dc_fetch_committees_data")

    # Step 1: Get initial page and cookies
    logger.info("Loading initial committee search page...")
    session = requests.Session()
    initial_response = session.get(
        DC_COMMITTEE_SEARCH_URL, headers=DC_COMMITTEE_HEADERS
    )
    initial_response.raise_for_status()

    # Step 2: Submit search form
    logger.info("Submitting committee search form...")
    submit_payload = {
        "CandidateMode": "1",
        "ElectionYear": "",
        "X-Requested-With": "XMLHttpRequest",
    }

    submit_headers = {
        **DC_COMMITTEE_HEADERS,
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": DC_COMMITTEE_BASE_URL,
        "referer": DC_COMMITTEE_SEARCH_URL,
        "x-requested-with": "XMLHttpRequest",
    }

    submit_response = session.post(
        DC_COMMITTEE_SUBMIT_URL, data=submit_payload, headers=submit_headers
    )
    submit_response.raise_for_status()

    # Step 3: Export to CSV
    logger.info("Exporting committee data to CSV...")
    export_url = f"{DC_COMMITTEE_BASE_URL}/ActiveCandidates/Export?exportType=CSV"

    export_headers = {
        "accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "cookie": "; ".join(
            [f"{k}={v}" for k, v in session.cookies.get_dict().items()]
        ),
        "if-modified-since": "Mon, 28 Apr 2025 01:56:40 GMT",
        "priority": "u=0, i",
        "sec-ch-ua": (
            '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'
        ),
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
            "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        ),
    }

    export_response = session.get(export_url, headers=export_headers)
    export_response.raise_for_status()

    # Step 4: Save results
    logger.info("Saving committee data...")
    output_dir = Path(DC_DATA_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "committees.csv"
    with open(output_file, "wb") as f:
        f.write(export_response.content)

    logger.info(f"Saved committee data to {output_file}")


@dg.asset(deps=[dc_fetch_committees_data])
def dc_insert_committees_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format DC committees.csv data to the right landing table"""

    dc_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="committees.csv",
        table_name="dc_committees_landing_table",
        table_columns_name=[
            "ocf_identification_no",
            "candidate_name",
            "committee_name",
            "election_year",
            "party",
            "office",
        ],
        data_validation_callback=lambda row: len(row) == 6,
    )

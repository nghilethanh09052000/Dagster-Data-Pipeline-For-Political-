import csv
import os
from collections.abc import Callable
from pathlib import Path

import dagster as dg
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

MN_DATA_PATH = f"{os.getcwd()}/.states/minnesota"
MN_TARGET_FILES_TO_FILE_ID = {
    "mn_contrib": "2113865252",
    "mn_general_expenditures": "1890073264",
    "mn_independent_expenditures": "617535497",
}


def get_url_for_minnesota_bulk_download_one_category(file_id: str) -> str:
    """
    Get minnesota bulk download URL for specific prefix (report type) for a cycle
    """
    return f"https://cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/?download=-{file_id}"


def mn_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    file_id: str,
    data_file_path: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Insert all available data files from one report type to its repsective postgres
    landing table. The function will ignore all missing year.

    This function assumed that all of the files have the same columns.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.
    start_year: the first year (the latest of the cycle year) the data is available,
                e.g. for candidate master is 1979-1980, so put in 1980
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

    logger = dg.get_dagster_logger(f"pa_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(MN_DATA_PATH)

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
            logger.warning(f"File for {file_id} is non-existent, ignoring...")
        except Exception as e:
            logger.error(f"Got error while reading {file_id} file: {e}")
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
def mn_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all missouri finance campaign report, such as
    - Itemized contributions received of over $200 (file_id: 2113865252)
    - Itemized general expenditures and contributions made of over $200
      (file_id: 1890073264)
    - Itemized independent expenditures of over $200 (file_id: 617535497)
    for all entities from 2015 - to present.
    """

    file_path = Path(MN_DATA_PATH)
    file_path.mkdir(parents=True, exist_ok=True)

    for file_name, file_id in MN_TARGET_FILES_TO_FILE_ID.items():
        context.log.info(f"Downloading campaign full export data for file_id {file_id}")

        url = get_url_for_minnesota_bulk_download_one_category(file_id=file_id)

        try:
            stream_download_file_to_path(
                request_url=url, file_save_path=file_path / f"{file_name}.csv"
            )

        except FileNotFoundError:
            context.log.info(f"File for id {file_id} is non-existent, ignoring... ")
        except Exception as e:
            context.log.info(f"Got error while reading {file_id} file: {e}")
            raise e


@dg.asset(deps=[mn_fetch_all_campaign_data_full_export])
def mn_insert_contrib_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MN mn_contrib.csv data to the right landing table"""

    mn_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        file_id=MN_TARGET_FILES_TO_FILE_ID["mn_contrib"],
        data_file_path="mn_contrib.csv",
        table_name="mn_contrib_landing_table",
        table_columns_name=[
            "Recipient reg num",
            "Recipient",
            "Recipient type",
            "Recipient subtype",
            "Amount",
            "Receipt date",
            "Year",
            "Contributor",
            "Contrib Reg Num",
            "Contrib type",
            "Receipt type",
            "In kind?",
            "In-kind descr",
            "Contrib zip",
            "Contrib Employer name",
        ],
        data_validation_callback=lambda row: len(row) == 15,
    )


@dg.asset(deps=[mn_fetch_all_campaign_data_full_export])
def mn_insert_general_expenditures_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert new format MN mn_general_expenditures.csv data to the right landing table
    """

    mn_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        file_id=MN_TARGET_FILES_TO_FILE_ID["mn_general_expenditures"],
        data_file_path="mn_general_expenditures.csv",
        table_name="mn_general_expenditures_landing_table",
        table_columns_name=[
            "Committee reg num",
            "Committee name",
            "Entity type",
            "Entity sub-type",
            "Vendor name",
            "Vendor address 1",
            "Vendor address 2",
            "Vendor city",
            "Vendor state",
            "Vendor zip",
            "Amount",
            "Unpaid amount",
            "Date",
            "Purpose",
            "Year",
            "Type",
            "In-kind descr",
            "In-kind?",
            "Affected committee name",
            "Affected committee reg num",
        ],
        data_validation_callback=lambda row: len(row) == 20,
    )


@dg.asset(deps=[mn_fetch_all_campaign_data_full_export])
def mn_insert_independent_expenditures_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert new format MN mn_independent_expenditures.csv data to the right landing table
    """

    mn_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        file_id=MN_TARGET_FILES_TO_FILE_ID["mn_independent_expenditures"],
        data_file_path="mn_independent_expenditures.csv",
        table_name="mn_independent_expenditures_landing_table",
        table_columns_name=[
            "Spender",
            "Spender Reg Num",
            "Spender type",
            "Spender sub-type",
            "Affected Comte Name",
            "Affected Cmte Reg Num",
            "For /Against",
            "Year",
            "Date",
            "Type",
            "Amount",
            "Unpaid amount",
            "In kind?",
            "In kind descr",
            "Purpose",
            "Vendor name",
            "Vendor address 1",
            "Vendor address 2",
            "Vendor city",
            "Vendor State",
            "Vendor zip",
        ],
        data_validation_callback=lambda row: len(row) == 21,
    )

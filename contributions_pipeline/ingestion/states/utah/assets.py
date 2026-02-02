import csv
import os
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup, element
from psycopg_pool import ConnectionPool
from requests import Response

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

BASE_URL = "https://disclosures.utah.gov"
UT_DATA_PATH = f"{os.getcwd()}/.states/utah/"

REPORT_COLUMNS = [
    "ENTITY",
    "REPORT",
    "TRAN_ID",
    "TRAN_TYPE",
    "TRAN_DATE",
    "TRAN_AMT",
    "INKIND",
    "LOAN",
    "AMENDS",
    "NAME",
    "PURPOSE",
    "ADDRESS1",
    "ADDRESS2",
    "CITY",
    "STATE",
    "ZIP",
    "INKIND_COMMENTS",
]


def get_next_page_details(
    payload: dict,
    page_number: int,
    url: str,
    session: requests.Session,
    response: Response,
    page_attempt: int,
) -> dict:
    page_attempt += 1
    page_info = {"status_code": [], "content_similarity": []}

    for page in range(1, page_attempt):
        payload["PageNumber"] = page_number + page
        temp = session.post(url=url, data=payload)
        page_info["status_code"].append(temp.status_code == 200)
        page_info["content_similarity"].append(response.content != temp.content)

    return page_info


def ut_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    data_file_path: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Insert all available data files from one report type to its respective postgres
    landing table. The function will ignore all missing year.

    This function assumed that all of the files have the same columns.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get access to the resource.
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

    logger = dg.get_dagster_logger(f"in_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(UT_DATA_PATH)

    first_page = 1
    max_page_inclusive = max(
        [int(dir.as_posix().split("_")[-1]) for dir in base_path.glob("*")]
    )
    step_size = 1

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for page in range(first_page, max_page_inclusive + 1, step_size):
            final_data_path = f"page_{page}/{data_file_path}"
            current_cycle_data_file_path = base_path / final_data_path

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

                # Ignore the headers
                next(parsed_current_cycle_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_current_cycle_file,
                    table_name=table_name,
                    table_columns_name=table_columns_name,
                    row_validation_callback=data_validation_callback,
                )

            except FileNotFoundError:
                logger.warning(f"File for page {page} is non-existent, ignoring...")
            except Exception as e:
                logger.error(f"Got error while reading cycle page {page} file: {e}")
                raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        if isinstance(count_cursor_result, list):
            cursor_result = int(count_cursor_result[0])
            row_count = cursor_result if count_cursor_result is not None else 0
        else:
            row_count = 0

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset()
def ut_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all utah finance campaign report
    """

    default_path = Path(UT_DATA_PATH)
    default_path.mkdir(parents=True, exist_ok=True)

    with requests.Session() as sess:
        page_number = 163

        get_entity_url = BASE_URL + "/Search/AdvancedSearch/GetEntityReportList"

        fetching = True

        while fetching:
            data = [",".join(REPORT_COLUMNS)]

            context.log.info(
                f"Bulk download campaign full export from page {page_number}."
            )

            file_path = default_path / f"page_{page_number}"
            file_path.mkdir(parents=True, exist_ok=True)

            payload = {
                "Search": "",
                "EntityType": "",
                "ReportYear": "2025",
                "HideContributions": "false",
                "HideExpenditures": "false",
                "PageNumber": f"{page_number}",
                "X-Requested-With": "XMLHttpRequest",
            }

            res = sess.post(url=get_entity_url, data=payload)

            soup = BeautifulSoup(res.content, "html.parser")

            for link in soup.find_all("a", href=True):
                if isinstance(link, element.Tag):
                    ahref = str(link.get("href"))
                    if "/Search/AdvancedSearch/GenerateReport/" in ahref:
                        report_url = BASE_URL + ahref
                        doc_response = sess.get(report_url, stream=True)

                        for chunk in doc_response.iter_content(chunk_size=1024):
                            chunk = chunk.decode("utf-8", "ignore").split("\n")
                            rows = [c.replace("0xe2", "") for c in chunk]
                            for row in rows[1:]:
                                if len(row.split(",")) == len(REPORT_COLUMNS):
                                    data.append(row)

            context.log.info(f"Total extracted data: {len(data)} rows")

            data = "\n".join(data).encode("utf-8")

            target_path = os.path.join(file_path, "UtFinanceReport.csv")

            with open(target_path, "wb") as csv_file:
                csv_file.write(data)

            page_info = get_next_page_details(
                payload=payload,
                page_number=page_number,
                url=get_entity_url,
                session=sess,
                response=res,
                page_attempt=5,
            )

            if any(page_info["status_code"]) and any(page_info["content_similarity"]):
                page_number += 1
            else:
                fetching = False


@dg.asset(deps=[ut_fetch_all_campaign_data_full_export])
def ut_insert_report_data_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format UT UtFinanceReport.csv data to the right landing table"""

    ut_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        data_file_path="UtFinanceReport.csv",
        table_name="ut_finance_report_landing_table",
        table_columns_name=[
            "ENTITY",
            "REPORT",
            "TRAN_ID",
            "TRAN_TYPE",
            "TRAN_DATE",
            "TRAN_AMT",
            "INKIND",
            "LOAN",
            "AMENDS",
            "NAME",
            "PURPOSE",
            "ADDRESS1",
            "ADDRESS2",
            "CITY",
            "STATE",
            "ZIP",
            "INKIND_COMMENTS",
        ],
        data_validation_callback=lambda row: len(row) == 17,
    )

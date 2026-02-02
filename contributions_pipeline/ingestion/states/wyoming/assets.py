import csv
import os
import ssl
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup, element
from fake_useragent import UserAgent
from psycopg_pool import ConnectionPool
from requests.adapters import HTTPAdapter

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

WY_DATA_PATH = f"{os.getcwd()}/.states/wyoming"
STATES_URL = "https://www.wycampaignfinance.gov/WYCFWebApplication"
REPORT_TYPE = ["Contributions", "Expenditures"]
INPUT_ELEMENTS = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
WY_FIRST_YEAR_OF_BULK_DATA_AVAILABLE = 2009
WY_LATEST_YEAR_OF_BULK_DATA_AVAILABLE = datetime.now().year + 1


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.set_ciphers("HIGH:!DH:!aNULL")
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)


def get_url_for_wy_bulk_download_one_category(report_type: str) -> str:
    """
    Get Wyoming bulk download URL for specific prefix (report type) for a cycle
    """

    return f"{STATES_URL}/GSF_SystemConfiguration/Search{report_type}.aspx"


def url_explorer(url_content: bytes, url: str, year: str, activity: str) -> dict:
    soup = BeautifulSoup(url_content, "html.parser")
    payload = {}

    page_elements = INPUT_ELEMENTS[:-1] if "Contributions" in url else INPUT_ELEMENTS

    for data in page_elements:
        page_el = soup.find("input", {"id": data})
        if isinstance(page_el, element.Tag) and page_el.has_attr("value"):
            payload.update({data: page_el["value"]})

    payload.update(
        {
            "__EVENTARGUMENT": "4",
            "ctl00$BodyContent$ddlCCOfficeSought": "-1",
            "ctl00$BodyContent$ddlCommitteeName": "-1",
            "ctl00$BodyContent$ddlCCParty": "0",
            "ctl00$BodyContent$Status": "rdoBothCC",
        }
    )

    yr_sel_form = "ctl00$BodyContent$txtElectionYearCC"
    if activity == "Search":
        payload.update(
            {yr_sel_form: str(year), "ctl00$BodyContent$bntSearch": activity}
        )

        return payload
    else:
        payload.update(
            {yr_sel_form: str(year), "ctl00$BodyContent$btnExport": activity}
        )

        return payload


def fetch_data_from_url(url: str, year: str, downloadables: Path) -> None:
    chrome = UserAgent().chrome

    with requests.Session() as session:
        session.mount("https://", TLSAdapter())
        session.headers = {
            "User-Agent": str(chrome),
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;"
            + "q=0.9,image/webp,image/apng,*/*;q=0.8,"
            + "application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7,ms;q=0.6,de;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
        }

        response_home = session.get(url)

        payload_search = url_explorer(
            url_content=response_home.content, url=url, year=year, activity="Search"
        )

        response_search = session.post(url=url, data=payload_search, stream=True)

        payload_export = url_explorer(
            url_content=response_search.content, url=url, year=year, activity="Export"
        )

        response_export = session.post(url=url, data=payload_export, stream=True)

        with open(downloadables, "wb") as f:
            for chunk in response_export.iter_content(chunk_size=8192):
                f.write(chunk)

        with open(downloadables) as f:
            if "<!DOCTYPE html>" in f.read():
                downloadables.unlink()


def wy_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
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

    logger = dg.get_dagster_logger(f"wy_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(WY_DATA_PATH)

    first_year = start_year
    last_year_inclusive = datetime.now().year
    step_size = 1

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for year in range(first_year, last_year_inclusive + 1, step_size):
            for report_type in REPORT_TYPE:
                current_file_path = base_path / f"{year}/{report_type}"
                current_cycle_data_file_path = current_file_path / data_file_path

                logger.info(
                    f"Inserting year {year} file to pg ({current_cycle_data_file_path})"
                )

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

                except StopIteration:
                    logger.warning(f"File for year {year} probably empty, ignoring...")
                    continue
                except FileNotFoundError:
                    logger.warning(f"File for year {year} is non-existent, ignoring...")
                except Exception as e:
                    logger.error(f"Got error while reading cycle year {year} file: {e}")
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
def wy_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext) -> None:
    """
    Main function to bulk download all wyoming finance campaign report.
    """

    for report_type in REPORT_TYPE:
        url = get_url_for_wy_bulk_download_one_category(report_type=report_type)

        for year in range(
            WY_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
            WY_LATEST_YEAR_OF_BULK_DATA_AVAILABLE,
        ):
            context.log.info(
                (
                    "Starting to download Wyoming Campaign Finance",
                    f"{year}_{report_type}",
                )
            )

            default_path = Path(WY_DATA_PATH) / f"{year}/{report_type}"
            default_path.mkdir(parents=True, exist_ok=True)

            fetch_data_from_url(
                url=url,
                year=str(year),
                downloadables=default_path / f"{report_type}.txt",
            )


@dg.asset(deps=[wy_fetch_all_campaign_data_full_export])
def wy_insert_contributions_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format WY contributions.txt data to the right landing table"""

    wy_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=WY_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="Contributions.txt",
        table_name="wy_contributions_landing_table",
        table_columns_name=[
            "Contributor Name",
            "Recipient Name",
            "Recipient Type",
            "Contribution Type",
            "Date",
            "Filing Status",
            "Amount",
            "City State Zip",
        ],
        data_validation_callback=lambda row: len(row) == 8,
    )


@dg.asset(deps=[wy_fetch_all_campaign_data_full_export])
def wy_insert_expenditures_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format WY expenditures.txt data to the right landing table"""

    wy_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=WY_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="Expenditures.txt",
        table_name="wy_expenditures_landing_table",
        table_columns_name=[
            "Filer Type",
            "Filer Name",
            "Payee",
            "Purpose",
            "Date",
            "City State Zip",
            "Filing Status",
            "Amount",
        ],
        data_validation_callback=lambda row: len(row) == 8,
    )

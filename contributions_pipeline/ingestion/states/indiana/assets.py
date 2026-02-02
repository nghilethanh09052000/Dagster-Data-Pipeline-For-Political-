import csv
import os
import shutil
import ssl
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
import requests
from bs4 import BeautifulSoup, element
from fake_useragent import UserAgent
from psycopg_pool import ConnectionPool
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from tqdm import tqdm

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

IN_STATE_URL = "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads"
IN_DATA_PATH = f"{os.getcwd()}/.states/indiana"
REPORT_TYPE = ["Contribution", "Expenditure"]
PAGE_ELEMENTS = [
    "_ctl0_Content_lblCommName",
    "_ctl0_Content_lblPhysAddress1",
    "_ctl0_Content_lblPhysCityStateZip",
    "_ctl0_Content_lblCommPhone",
    "_ctl0_Content_lblCommitteeType",
    "_ctl0_Content_lblCommParty",
]
IN_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE = 2000


def get_url_for_indiana_bulk_download_one_cycle(year: str, report_type: str) -> str:
    """
    Get Indiana bulk download URL for specific prefix (report type) for a cycle

    Parameters:
    year (int): the year the full export going to be downloaded
    """

    return f"{IN_STATE_URL}/{year}_{report_type}Data.csv.zip"


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.set_ciphers("HIGH:!DH:!aNULL")
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)


def get_indiana_committee_data(org_id: str) -> list:
    url = f"https://campaignfinance.in.gov/PublicSite/SearchPages/CommitteeDetail.aspx?OrgID={org_id}"

    with requests.Session() as sess:
        sess.mount("https://", TLSAdapter())
        headers = {"User-Agent": str(UserAgent().chrome)}

        response = sess.get(url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")

        data = []
        for el in PAGE_ELEMENTS:
            entity = soup.find("span", {"id": el})
            if isinstance(entity, element.Tag):
                if "CityStateZip" in el:
                    address = entity.getText().split(" ")

                    zip_code = address[-1] if len(address) > 2 else ""
                    state = address[-2] if len(address) > 2 else ""
                    city = " ".join(address[:-2]) if len(address) > 2 else ""

                    data.extend([city, state, zip_code])

                else:
                    el_text = entity.getText()
                    data.append(el_text)
            else:
                data.append("")

        return data


def fetch_committee_data(file_path: Path, file_number: list) -> None:
    committee_data = [
        [
            "comm_name",
            "comm_type",
            "street_address",
            "city",
            "state",
            "zip_code",
            "phone",
            "party",
        ]
    ]

    comittee_profiles = list(set(file_number))
    for i in tqdm(
        range(0, len(comittee_profiles)), desc="Scrape Committee Profile Data"
    ):
        try:
            row = get_indiana_committee_data(org_id=comittee_profiles[i])
            committee_data.append(row)
        except ConnectionError:
            continue

    committee_data_path = file_path / "Committee"
    committee_data_path.mkdir(parents=True, exist_ok=True)

    with open(committee_data_path / "CommitteeData.csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(committee_data)


def in_insert_files_to_landing_of_one_type(
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

    logger = dg.get_dagster_logger(f"in_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(IN_DATA_PATH)

    first_year = start_year
    last_year_inclusive = datetime.now().year
    step_size = 1

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        if "Committee" in data_file_path:
            final_data_path = f"Committee/{data_file_path}"
            current_cycle_data_file_path = base_path / final_data_path

            logger.info(f"Inserting file to pg ({current_cycle_data_file_path})")

            try:
                current_cycle_file_lines_generator = safe_readline_csv_like_file(
                    file_path=current_cycle_data_file_path
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
                logger.warning("File Committee Data is non-existent, ignoring...")
            except Exception as e:
                logger.error(f"Got error while reading Committee Data file: {e}")
                raise e

        else:
            for year in range(first_year, last_year_inclusive + 1, step_size):
                for report_type in REPORT_TYPE:
                    final_data_path = f"{year}/{report_type}/{data_file_path}"
                    current_cycle_data_file_path = base_path / final_data_path

                    logger.info(
                        f"Inserting file to pg ({current_cycle_data_file_path})"
                    )

                    try:
                        current_file_lines_generator = safe_readline_csv_like_file(
                            file_path=current_cycle_data_file_path, encoding="utf-8"
                        )
                        parsed_current_cycle_file = csv.reader(
                            current_file_lines_generator,
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
                        logger.warning(
                            f"File for year {year} is non-existent, ignoring..."
                        )
                    except Exception as e:
                        logger.error(
                            f"Got error while reading cycle year {year} file: {e}"
                        )
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
def in_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all indiana finance campaign report
    """

    file_path = Path(IN_DATA_PATH)
    file_path.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": str(UserAgent().chrome),
        "Upgrade-Insecure-Requests": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;"
        + "q=0.9,image/webp,image/apng,*/*;q=0.8,"
        + "application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    first_year = IN_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE
    last_year_inclusive = datetime.now().year

    file_number = []
    for year in range(first_year, last_year_inclusive + 1):
        for report_type in REPORT_TYPE:
            context.log.info(
                f"Downloading campaign full export {report_type} data for year {year}"
            )

            temp_full_export_zip_file_path = file_path / f"{report_type}Data.csv.zip"

            url = get_url_for_indiana_bulk_download_one_cycle(
                year=str(year), report_type=report_type
            )

            try:
                stream_download_file_to_path(
                    request_url=url,
                    file_save_path=temp_full_export_zip_file_path,
                    headers=headers,
                )

                year_data_folder_path = file_path / f"{year}/{report_type}/"
                year_data_folder_path.mkdir(parents=True, exist_ok=True)

                with ZipFile(temp_full_export_zip_file_path, "r") as temp_export_zip:
                    for file_info in temp_export_zip.infolist():
                        cleaned_filename = file_info.filename.split("/")[-1]
                        cleaned_filename = cleaned_filename.split("_")[-1]
                        cleaned_filename = cleaned_filename.removesuffix(".csv")
                        cleaned_filename = cleaned_filename + ".csv"

                        target_path = os.path.join(
                            year_data_folder_path, cleaned_filename
                        )

                        # Ignore any folders, flatten all files to the root export path
                        if file_info.filename.endswith("/"):
                            continue

                        # Extract file using buffered streaming
                        with (
                            temp_export_zip.open(file_info) as source,
                            open(target_path, "wb") as target,
                        ):
                            shutil.copyfileobj(source, target, 65536)  # 64KB buffer

                        if report_type == "Contribution":
                            csv_generator = safe_readline_csv_like_file(
                                file_path=target_path, encoding="utf-8"
                            )

                            csv_in = csv.reader(
                                csv_generator,
                                delimiter=",",
                                quotechar='"',
                            )
                            next(csv_in)
                            for row in csv_in:
                                if len(row) > 0:
                                    org_id = row[0]
                                    file_number.append(str(org_id))

            except RuntimeError as re:
                context.log.warning(
                    f"Failed to process {report_type} data in year {year}: {re}"
                )
                continue
            finally:
                temp_full_export_zip_file_path.unlink(missing_ok=True)

    context.log.info(
        f"Downloading Indiana committee data full export. {len(list(set(file_number)))}"
    )
    fetch_committee_data(file_path=file_path, file_number=file_number)


@dg.asset(deps=[in_fetch_all_campaign_data_full_export])
def in_insert_contribution_data_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format IN ContributionData.csv data to the right landing table"""

    in_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=IN_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path="ContributionData.csv",
        table_name="in_contribution_data_landing_table",
        table_columns_name=[
            "FileNumber",
            "CommitteeType",
            "Committee",
            "CandidateName",
            "ContributorType",
            "Name",
            "Address",
            "City",
            "State",
            "Zip",
            "Occupation",
            "Type",
            "Description",
            "Amount",
            "ContributionDate",
            "Received_By",
            "Amended",
        ],
        data_validation_callback=lambda row: len(row) == 17,
    )


@dg.asset(deps=[in_fetch_all_campaign_data_full_export])
def in_insert_expenditure_data_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format IN ExpenditureData.csv data to the right landing table"""

    in_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=IN_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path="ExpenditureData.csv",
        table_name="in_expenditure_data_landing_table",
        table_columns_name=[
            "FileNumber",
            "CommitteeType",
            "Committee",
            "CandidateName",
            "ExpenditureCode",
            "Name",
            "Address",
            "City",
            "State",
            "Zip",
            "Occupation",
            "OfficeSought",
            "ExpenditureType",
            "Description",
            "Purpose",
            "Amount",
            "Expenditure_Date",
            "Amended",
        ],
        data_validation_callback=lambda row: len(row) == 18,
    )


@dg.asset(deps=[in_fetch_all_campaign_data_full_export])
def in_insert_committee_data_to_landing(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert new format IN CommitteeData.csv data to the right landing table"""

    in_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=IN_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path="CommitteeData.csv",
        table_name="in_committee_data_landing_table",
        table_columns_name=[
            "comm_name",
            "street_address",
            "city",
            "state",
            "zip_code",
            "phone",
            "comm_type",
            "party",
        ],
        data_validation_callback=lambda row: len(row) == 8,
    )

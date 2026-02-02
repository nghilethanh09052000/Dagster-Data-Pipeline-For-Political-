import time
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import save_parse_excel
from contributions_pipeline.lib.ingest import (
    COPY_INSERT_BATCH_NUM,
    LOG_INVALID_ROWS,
    get_sql_binary_format_copy_query_for_landing_table,
    get_sql_count_query,
    get_sql_truncate_query,
)
from contributions_pipeline.lib.iter import batched
from contributions_pipeline.resources import PostgresResource

OR_DATA_BASE_PATH = "./states/oregon"
OR_FIRST_YEAR_DATA_AVAILABLE = 1998


def or_fetch_all_files_from_storage(base_url: str, start_year: int, file_name: str):
    """
    This funciton is going to dowload all the files from a static download site.
    The expected URL format is "{base_url}/{year}/XcelCNESearch.xls".
    If there's 404 on any of the file_name, the function will only logs it,
    no error will be raised.

    Params:
    base_url: base url of the storage
        start_year: the first year you want to check for all of the file names in the
                base url, this will end in the current year and month
    """

    logger = dg.get_dagster_logger("vt_fetch_from_storage")
    base_path = Path(OR_DATA_BASE_PATH)

    last_year_inclusive = datetime.now().year

    for year in range(start_year, last_year_inclusive + 1):
        year_data_base_path = base_path / str(year)
        year_data_base_path.mkdir(parents=True, exist_ok=True)

        with requests.session() as req_session:
            file_download_uri = f"{base_url}/{year}/{file_name}"

            logger.info(f"Downloading on year {year} ({file_download_uri})")

            with req_session.get(file_download_uri, stream=True) as response:
                if response.status_code == 404 or response.status_code == 400:
                    logger.warning(
                        f"File on year {year} not found (URL: {file_download_uri}"
                    )
                    continue

                response.raise_for_status()

                file_save_path = year_data_base_path / file_name
                logger.info(f"Saving on year {year} to {file_save_path}")

                with open(file_save_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)


def or_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
    file_name: str,
    table_name: str,
    table_columns_name: list[str],
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
    file_name: the file path of the actual data file on the year folder, for
               example contributions, the local folder for it is laid out as it
               follows (year) -> contrib.txt. The data file is itcont.txt,
               thus the input should be "contrib.txt".
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted.
    """

    logger = dg.get_dagster_logger(f"or_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)
    landing_table_copy_query = get_sql_binary_format_copy_query_for_landing_table(
        table_name=table_name, table_columns_name=table_columns_name
    )

    base_path = Path(OR_DATA_BASE_PATH)

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
            current_cycle_data_file_path = base_path / f"{year}/{file_name}"

            logger.info(
                f"Inserting year {year} file to pg ({current_cycle_data_file_path})"
            )

            try:
                parsed_current_year_file_generator = save_parse_excel(
                    file_path=current_cycle_data_file_path
                )
                # Ignore the header
                next(parsed_current_year_file_generator)

                for chunk_rows in batched(
                    iterable=parsed_current_year_file_generator, n=COPY_INSERT_BATCH_NUM
                ):
                    chunk_len = len(chunk_rows)
                    start_time = time.time()
                    with pg_cursor.copy(
                        statement=landing_table_copy_query
                    ) as copy_cursor:
                        for row_number, row_data in enumerate(chunk_rows):
                            if len(row_data) != len(table_columns_name):
                                if LOG_INVALID_ROWS:
                                    logger.warning(
                                        (
                                            f"Row no.{row_number + 1} is not valid.",
                                            f"Parsed data: {row_data}",
                                        )
                                    )
                                continue

                            copy_cursor.write_row(row_data)

                elapsed = time.time() - start_time

                logger.info(f"batch done: {round(chunk_len / elapsed, 3)} rows/s")

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
def or_fetch_contributions_data():
    """
    Download all available hand-scraped data from Oregon SOS website
    """

    or_fetch_all_files_from_storage(
        base_url="https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/or",
        start_year=OR_FIRST_YEAR_DATA_AVAILABLE,
        file_name="XcelCNESearch.xls",
    )


@dg.asset()
def or_fetch_committees_data():
    """
    Download all available hand-scraped data from Oregon SOS website
    """

    or_fetch_all_files_from_storage(
        base_url="https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/or",
        start_year=OR_FIRST_YEAR_DATA_AVAILABLE,
        file_name="XcelSooSearch.xls",
    )


@dg.asset(deps=[or_fetch_contributions_data])
def or_insert_contributions_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all contributions data from all year to landing table.
    """

    or_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=OR_FIRST_YEAR_DATA_AVAILABLE,
        file_name="XcelCNESearch.xls",
        table_name="or_transaction_landing",
        table_columns_name=[
            "TranId",
            "OriginalId",
            "TranDate",
            "TranStatus",
            "Filer",
            "ContributorOrPayee",
            "SubType",
            "PayerofPersonalExpenditure",
            "Amount",
            "AggregateAmount",
            "ContributorOrPayeeCommitteeID",
            "FilerId",
            "AttestByName",
            "AttestDate",
            "ReviewByName",
            "ReviewDate",
            "DueDate",
            "OccptnLtrDate",
            "PymtSchedTxt",
            "PurpDesc",
            "IntrstRate",
            "CheckNbr",
            "TranStsfdInd",
            "FiledByName",
            "FiledDate",
            "AddrbookAgentName",
            "BookType",
            "TitleTxt",
            "OccptnTxt",
            "EmpName",
            "EmpCity",
            "EmpState",
            "EmployInd",
            "SelfEmployInd",
            "AddrLine1",
            "AddrLine2",
            "City",
            "State",
            "Zip",
            "ZipPlusFour",
            "County",
            "Country",
            "ForeignPostalCode",
            "PurposeCodes",
            "ExpDate",
        ],
    )


@dg.asset(deps=[or_fetch_committees_data])
def or_insert_commitees_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all commitees data from all year to landing table.
    """

    or_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=OR_FIRST_YEAR_DATA_AVAILABLE,
        file_name="XcelSooSearch.xls",
        table_name="or_committees_landing",
        table_columns_name=[
            "CommitteeId",
            "CommitteeName",
            "CommitteeType",
            "CommitteeSubType",
            "CandidateOffice",
            "CandidateOfficeGroup",
            "FilingDate",
            "OrganizationFilingDate",
            "TreasurerFirstName",
            "TreasurerLastName",
            "TreasurerMailingAddress",
            "TreasurerWorkPhone",
            "TreasurerFax",
            "CandidateFirstName",
            "CandidateLastName",
            "CandidateMalingAddress",
            "CandidateWorkPhone",
            "CandidateResidencePhone",
            "CandidateFax",
            "CandidateEmail",
            "ActiveElection",
            "Measure",
        ],
    )

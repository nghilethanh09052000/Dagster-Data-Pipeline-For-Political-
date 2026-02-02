import csv
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
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

COLORADO_DATA_BASE_PATH = "./states/colorado"
COLORADO_FIRST_YEAR_TRACER_DATA_AVAILABLE = datetime(2000, 1, 1)


# ===== Partition Definition =====
co_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=COLORADO_FIRST_YEAR_TRACER_DATA_AVAILABLE,
    end_offset=1,
)


def colorado_get_data_download_url_for_one_report_type(
    colorado_report_suffix: str, year: int
) -> str:
    """
    Get colorado TRACER archive download URL for specific year and report suffix.

    Parameters:
    year: the year you want to download the data
    colorado_report_suffix: data prefix between year and file format. e.g. for
                            contributions, the data download url is
                            ".../BulkDataDownloads/2025_ContributionData.csv.zip",
                            so the the value of this parameters should be
                            "ContributionData".
    Returns: full URL to download the data form Colorado TRACER system
    """
    return f"https://tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads/{year}_{colorado_report_suffix}.csv.zip"


def co_fetch_one_report_type(
    year: int, colorado_report_suffix: str, human_friendly_name: str
):
    """
    Fetch all the reports from start year until current year.

    Using campaign finance data download from Colorado TRACER, this function
    will loop from the first year given in parameters, until the current year.
    For each year the function will:
        1. Download the zip to data year folder temporarly
        2. Unzip the file to the correct year-prefixed data folder
        3. Deletes the temporary zip

    Parameters:
    year: when does the data are first available, inclusive
    colorado_report_suffix: data prefix between year and file format. e.g. for
                            contributions, the data download url is
                            ".../BulkDataDownloads/2025_ContributionData.csv.zip",
                            so the the value of this parameters should be
                            "ContributionData".
    human_friendly_name: human friendly name to be used on loggin. e.g. for
                         contributions you can put this parameters as
                         "Contributions".
    """

    logger = dg.get_dagster_logger(f"co_{colorado_report_suffix}_fetch")

    base_path = Path(COLORADO_DATA_BASE_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    archive_download_url = colorado_get_data_download_url_for_one_report_type(
        colorado_report_suffix=colorado_report_suffix, year=year
    )
    year_data_path = base_path / str(year)
    year_data_path.mkdir(parents=True, exist_ok=True)

    year_temp_archive_path = (
        year_data_path / f"{year}_{colorado_report_suffix}_temp.zip"
    )

    try:
        logger.info(
            (
                f"Getting {human_friendly_name} data on year {year}",
                f"URL: {archive_download_url}",
            )
        )
        stream_download_file_to_path(
            request_url=archive_download_url,
            file_save_path=year_temp_archive_path,
            verify=False,
        )

        logger.info(f"Unzipping {human_friendly_name} ({year}) on {year_data_path}")

        with ZipFile(file=year_temp_archive_path, mode="r") as temp_zip:
            temp_zip.extractall(year_data_path)
    finally:
        year_temp_archive_path.unlink(missing_ok=True)


def co_get_raw_data_path_by_year_and_suffix(
    year: int, colorado_report_suffix: str
) -> Path:
    """
    Get path to raw data based on the year and the data suffix.

    Parameters:
    start_year: the year of data reporting
    colorado_report_suffix: data prefix between year and file format. e.g. for
                            contributions, the data download url is
                            ".../BulkDataDownloads/2025_ContributionData.csv.zip",
                            so the the value of this parameters should be
                            "ContributionData".
    """

    base_path = Path(COLORADO_DATA_BASE_PATH)

    return base_path / f"{year}/{year}_{colorado_report_suffix}.csv"


def co_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    colorado_report_suffix: str,
    year: int,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    truncate_query: sql.Composed | None = None,
) -> dg.MaterializeResult:
    """
    Insert raw Colorado state TRACER data to landing table in Postgres.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.
    colorado_report_suffix: data prefix between year and file format. e.g. for
                            contributions, the data download url is
                            ".../BulkDataDownloads/2025_ContributionData.csv.zip",
                            so the the value of this parameters should be
                            "ContributionData".
    year: when does the data of year are available, inclusive
    table_name: name of the landing table the data will be inserted into,
                e.g. "co_contributions"
    table_columns_name: list of columns name, be sure to have the table column be in
                        the same order as the file columns order as well.
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    truncate_query: Optional custom truncate query that used for delete specific year

    """

    logger = dg.get_dagster_logger(name=f"ca_{colorado_report_suffix}_insert")

    if truncate_query is None:
        truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        try:
            raw_data_path = co_get_raw_data_path_by_year_and_suffix(
                year=year, colorado_report_suffix=colorado_report_suffix
            )

            data_type_lines_generator = safe_readline_csv_like_file(raw_data_path)
            parsed_data_type_file = csv.reader(
                data_type_lines_generator,
                delimiter=",",
                quotechar='"',
            )

            # Skip the header
            next(parsed_data_type_file)

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_data_type_file,
                table_name=table_name,
                table_columns_name=table_columns_name,
                row_validation_callback=data_validation_callback,
            )

        except FileNotFoundError:
            logger.warning(
                f"File for data {colorado_report_suffix} is non-existent, ignoring!"
            )
        except Exception as e:
            logger.error(f"Got error while reading {colorado_report_suffix} file: {e}")
            raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset(partitions_def=co_yearly_partition)
def co_contributions(context: dg.AssetExecutionContext):
    """Fetching all colorado contributions data from TRACER on all the available year"""
    year = int(context.partition_key)
    co_fetch_one_report_type(
        year=year,
        colorado_report_suffix="ContributionData",
        human_friendly_name="Contributions Data",
    )


@dg.asset(deps=[co_contributions], partitions_def=co_yearly_partition)
def co_contributions_insert_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """Insert Colorado Contributions data to landing table"""
    year = int(context.partition_key)
    table_name = "co_contributions_landing"
    table_name_identifier = sql.Identifier(table_name)
    truncate_query = sql.SQL(
        'DELETE FROM {table_name} WHERE LEFT("ContributionDate", 4) = {year_str}'
    ).format(table_name=table_name_identifier, year_str=sql.Literal(str(year)))

    co_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        colorado_report_suffix="ContributionData",
        year=year,
        table_name="co_contributions_landing",
        table_columns_name=[
            "CO_ID",
            "ContributionAmount",
            "ContributionDate",
            "LastName",
            "FirstName",
            "MI",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Explanation",
            "RecordID",
            "FiledDate",
            "ContributionType",
            "ReceiptType",
            "ContributorType",
            "Electioneering",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "Employer",
            "Occupation",
            "Amended",
            "Amendment",
            "AmendedRecordID",
            "Jurisdiction",
            "OccupationComments",
        ],
        data_validation_callback=lambda row: len(row) == 29,
        truncate_query=truncate_query,
    )


@dg.asset(
    partitions_def=co_yearly_partition,
)
def co_loans(context: dg.AssetExecutionContext):
    """Fetching all colorado loans data from TRACER on all the available year"""
    year = int(context.partition_key)
    co_fetch_one_report_type(
        year=year,
        colorado_report_suffix="LoanData",
        human_friendly_name="Loans Data",
    )


@dg.asset(
    deps=[co_loans],
    partitions_def=co_yearly_partition,
)
def co_loans_insert_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """Insert Colorado Loans data to landing table for the given partition year"""
    year = int(context.partition_key)
    table_name = "co_loans_landing"
    table_name_identifier = sql.Identifier(table_name)

    truncate_query = sql.SQL(
        'DELETE FROM {table_name} WHERE LEFT("PaymentDate", 4) = {year_str}'
    ).format(
        table_name=table_name_identifier,
        year_str=sql.Literal(str(year)),
    )

    co_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        colorado_report_suffix="LoanData",
        year=year,
        table_name=table_name,
        table_columns_name=[
            "CO_ID",
            "PaymentAmount",
            "PaymentDate",
            "Name",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Description",
            "RecordID",
            "FiledDate",
            "Type",
            "LoanSourceType",
            "LoanAmount",
            "LoanDate",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "InterestRate",
            "InterestPayment",
            "Amended",
            "Amendment",
            "AmendedRecordID",
            "Jurisdiction",
            "LoanBalance",
        ],
        data_validation_callback=lambda row: len(row) == 26,
        truncate_query=truncate_query,
    )


@dg.asset(partitions_def=co_yearly_partition)
def co_expenditures(context: dg.AssetExecutionContext):
    """
    Fetching all colorado expenditures data
    from TRACER on all the available year
    """
    year = int(context.partition_key)
    co_fetch_one_report_type(
        year=year,
        colorado_report_suffix="ExpenditureData",
        human_friendly_name="Expenditures Data",
    )


@dg.asset(
    deps=[co_expenditures],
    partitions_def=co_yearly_partition,
)
def co_expenditures_insert_to_landing_table(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert Colorado Expenditures data to landin
    table for the given partition year
    """
    year = int(context.partition_key)
    table_name = "co_expenditures_landing"
    table_name_identifier = sql.Identifier(table_name)

    truncate_query = sql.SQL(
        'DELETE FROM {table_name} WHERE LEFT("ExpenditureDate", 4) = {year_str}'
    ).format(
        table_name=table_name_identifier,
        year_str=sql.Literal(str(year)),
    )

    co_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        colorado_report_suffix="ExpenditureData",
        year=year,
        table_name=table_name,
        table_columns_name=[
            "CO_ID",
            "ExpenditureAmount",
            "ExpenditureDate",
            "LastName",
            "FirstName",
            "MI",
            "Suffix",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
            "Explanation",
            "RecordID",
            "FiledDate",
            "ExpenditureType",
            "PaymentType",
            "DisbursementType",
            "Electioneering",
            "CommitteeType",
            "CommitteeName",
            "CandidateName",
            "Employer",
            "Occupation",
            "Amended",
            "Amendment",
            "AmendedRecordID",
            "Jurisdiction",
        ],
        data_validation_callback=lambda row: len(row) == 28,
        truncate_query=truncate_query,
    )

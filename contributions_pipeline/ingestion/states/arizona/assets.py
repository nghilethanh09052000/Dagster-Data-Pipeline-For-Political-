import csv
from collections.abc import Generator
from datetime import datetime
from ftplib import FTP
from logging import Logger
from pathlib import Path

import dagster as dg
import requests
from psycopg import sql
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

FTP_HOST = "ftp.azsos.gov"
FTP_DIR = "cfsdata"
AZ_DATA_PATH_PREFIX = Path("./states/arizona")
AZ_FIRST_YEAR_DATA_AVAILABLE = datetime(year=2016, month=1, day=1)

# URLs for Arizona contributions
AZ_CONTRIBUTIONS_BASE_URL = "https://seethemoney.az.gov"
AZ_CONTRIBUTIONS_EXPORT_URL = (
    f"{AZ_CONTRIBUTIONS_BASE_URL}/Reporting/ExportEntityOverview"
)

# Headers for requests
AZ_CONTRIBUTIONS_HEADERS = {
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


def list_files(ftp: FTP, remote_dir: str, logger) -> list[str]:
    """
    List only .txt files in a given remote directory on the FTP server.

    Parameters:
    - ftp (FTP): Active FTP connection.
    - remote_dir (str): Path to the directory on the FTP server.
    - logger (Logger): Logger for logging information.

    Returns:
    - list[str]: A list of .txt filenames found in the directory.
    """
    logger.info("Start List Files On FTP")
    try:
        items = ftp.nlst(remote_dir)
        return [item.split("/")[-1] for item in items if item.lower().endswith(".txt")]
    except Exception as e:
        logger(f"Error listing {remote_dir}: {e}")
        return []


def download_file(ftp: FTP, remote_file: str, local_dir: Path, logger: Logger):
    """
    Download a specific file from the FTP server without changing directories.

    Parameters:
    - ftp (FTP): Active FTP connection.
    - remote_file (str): Full path of the file on the FTP server.
    - local_dir (Path): Local directory to save the downloaded file.
    - logger (Logger): Logger for logging information.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    local_file_path = local_dir / Path(remote_file).name

    logger.info(f"Start Download On Remote: {remote_file}")

    try:
        with open(local_file_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_file}", f.write)
        logger.info(f"Downloaded: {remote_file} -> {local_file_path}")
    except Exception as e:
        logger.error(f"Failed to download {remote_file}: {e}")


def az_insert_raw_file_to_landing_table(
    name: str,
    postgres_pool: ConnectionPool,
    table_name: str,
    table_columns_name: list[str],
    data_files: list[str],
    truncate_query: sql.Composed | None = None,
) -> dg.MaterializeResult:
    """
    Insert raw Arizona campaign finance data into the PostgreSQL landing table.

    Parameters:
    - postgres_pool (ConnectionPool): PostgreSQL connection pool.

    - filename (str): Name of the data file being processed. For example:
            AllowedInitialTransactions.txt, AllowedOffsetTransactions.txt ...

    - table_name (str): Name of the target PostgreSQL table just like the
                        migration files. For example:
                        + az_allowed_initial_transactions_landing
                        + az_allowed_offset_transactions_landing
                        ...

    - table_columns_name (list[str]): List of column names for insertion.

    - data_file (str): Path to the data file.

    Returns:
    - dg.MaterializeResult: Dagster result containing metadata on inserted rows.
    """

    logger = dg.get_dagster_logger(name=f"az_{name}_insert")

    def cleaned_parsed_line(data_file: str) -> Generator[str, None, None]:
        """
        Ignore empty lines after readline
        """
        data_type_lines_generator = safe_readline_csv_like_file(data_file)

        for line in data_type_lines_generator:
            if len(line.strip()) < 1:
                continue

            yield line

    def parse_data_files() -> Generator[list[str], None, None]:
        """
        Parse all files to one generator, so we can chunk multiple
        files to insert at once. Should be a litte bit faster than
        opening new copy query for each (relatively) small files
        """
        for data_file in data_files:
            logger.info(f"Processing file: {data_file}")
            data_type_lines_generator = cleaned_parsed_line(data_file)

            # Read as tab-separated, comma-separated values
            logger.info(f"Processing parse data file: {data_file}")
            parsed_data_type_file = csv.reader(
                data_type_lines_generator,
                delimiter="\t" if data_file.split(".")[-1] == "txt" else ",",
            )

            try:
                # Skip the header
                next(parsed_data_type_file, None)
            except StopIteration:
                logger.warning(f"File {data_file} probably empty, ignoring...")
                continue

            yield from parsed_data_type_file

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name} before inserting new data.")
        pg_cursor.execute(
            truncate_query
            if truncate_query is not None
            else get_sql_truncate_query(table_name=table_name)
        )

        all_file_parsed_generator = parse_data_files()

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=all_file_parsed_generator,
            table_name=table_name,
            table_columns_name=table_columns_name,
            row_validation_callback=lambda row: len(row) == len(table_columns_name),
        )

        # Verify row count
        count_query = get_sql_count_query(table_name=table_name)

        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Successfully inserted {row_count} rows into {table_name}")

        return dg.MaterializeResult(metadata={"dagster/table_name": table_name})


@dg.asset()
def az_fetch_campaign_finance():
    """Fetches all .txt files from the Arizona FTP server and saves them locally."""

    logger = dg.get_dagster_logger(name="az_fetch_campaign_finance")

    with FTP(FTP_HOST) as ftp:
        ftp.login()
        files = list_files(ftp, FTP_DIR, logger)

        for file in files:
            remote_file_path = f"{FTP_DIR}/{file}"
            download_file(ftp, remote_file_path, AZ_DATA_PATH_PREFIX, logger)


@dg.asset(deps=[az_fetch_campaign_finance])
def az_allowed_initial_transactions_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts AllowedInitialTransactions.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="AllowedInitialTransactions.txt",
        postgres_pool=pg.pool,
        table_name="az_allowed_initial_transactions_landing",
        table_columns_name=[
            "transaction_type_id",
            "name_type",
            "payment_type",
            "resulting_transaction_description",
            "combo_reference",
        ],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/AllowedInitialTransactions.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_allowed_offset_transactions_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts AllowedOffsetTransactions.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="AllowedOffsetTransactions.txt",
        postgres_pool=pg.pool,
        table_name="az_allowed_offset_transactions_landing",
        table_columns_name=[
            "transaction_type_id",
            "offsets_combo_reference",
            "resulting_transaction_description",
        ],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/AllowedOffsetTransactions.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_committees_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts Committees.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="Committees.txt",
        postgres_pool=pg.pool,
        table_name="az_committees_landing",
        table_columns_name=[
            "committee_id",
            "committee_name",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "committee_type",
            "organization_date",
            "termination_date",
        ],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/Committees.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_counties_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts Counties.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="Counties.txt",
        postgres_pool=pg.pool,
        table_name="az_counties_landing",
        table_columns_name=["county_id", "county_name"],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/Counties.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_reporting_periods_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts ReportingPeriods.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="ReportingPeriods.txt",
        postgres_pool=pg.pool,
        table_name="az_reporting_periods_landing",
        table_columns_name=[
            "cycle",
            "report_name",
            "reporting_period_begin_date",
            "reporting_period_end_date",
            "filing_period_begin_date",
            "filing_period_end_date",
            "report_type",
        ],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/ReportingPeriods.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_transaction_types_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts TransactionTypes.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="TransactionTypes.txt",
        postgres_pool=pg.pool,
        table_name="az_transaction_types_landing",
        table_columns_name=[
            "transaction_type_id",
            "transaction_type_name",
            "is_initial",
        ],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/TransactionTypes.txt"],
    )


@dg.asset(deps=[az_fetch_campaign_finance])
def az_transaction_types_allowed_by_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Inserts TransactionTypesAllowedByCommittee.txt data into the landing table.
    """
    return az_insert_raw_file_to_landing_table(
        name="TransactionTypesAllowedByCommittee",
        postgres_pool=pg.pool,
        table_name="az_transaction_types_allowed_by_committee_landing",
        table_columns_name=["combo_reference", "committee_type"],
        data_files=[f"{AZ_DATA_PATH_PREFIX}/TransactionTypesAllowedByCommittee.txt"],
    )


az_contributions_daily_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=AZ_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(
    partitions_def=az_contributions_daily_partition,
)
async def az_fetch_contributions(context: dg.AssetExecutionContext) -> None:
    """
    Fetch contributions data for Arizona.
    This asset is partitioned by year, fetching data for each year.
    """
    logger = context.log
    year = int(context.partition_key)

    session = requests.Session()

    output_dir = AZ_DATA_PATH_PREFIX / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Fetching contributions for {year}...")

    base_url = "https://seethemoney.az.gov/Reporting/GetNEWTableData/"

    # Initial parameters for URL
    url_params = {
        "Page": "7",
        "startYear": str(year),
        "endYear": str(year),
        "JurisdictionId": "0|Page",
        "TablePage": "1",
        "TableLength": "10",
        "IsLessActive": "false",
        "ShowOfficeHolder": "false",
        "ChartName": "7",
    }

    start = 0
    while True:
        try:
            # Prepare payload for POST request
            payload = {
                "draw": "1",
                "columns[0][data]": "EntityLastName",
                "columns[0][name]": "",
                "columns[0][searchable]": "true",
                "columns[0][orderable]": "true",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
                "columns[1][data]": "Income",
                "columns[1][name]": "",
                "columns[1][searchable]": "true",
                "columns[1][orderable]": "true",
                "columns[1][search][value]": "",
                "columns[1][search][regex]": "false",
                "order[0][column]": "1",
                "order[0][dir]": "desc",
                "start": str(start),
                "length": "100",
                "search[value]": "",
                "search[regex]": "false",
            }

            # Fetch entity data using POST
            response = session.post(
                base_url,
                params=url_params,
                data=payload,
                headers={
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "accept-encoding": "gzip, deflate, br, zstd",
                    "accept-language": "en-US,en;q=0.9",
                    "content-type": (
                        "application/x-www-form-urlencoded; charset=UTF-8"
                    ),
                    "origin": "https://seethemoney.az.gov",
                    "referer": "https://seethemoney.az.gov/Reporting/Explore",
                    "user-agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
                    ),
                    "x-requested-with": "XMLHttpRequest",
                },
            )
            response.raise_for_status()

            # Parse the response
            data = response.json()

            if not data.get("data"):
                logger.info(f"No more data for {year}")
                break

            # Process each entity
            for entity in data["data"]:
                entity_id = entity["EntityID"]

                logger.info(f"Processing entity {entity_id}")

                # Export URL for detailed transactions
                export_url = (
                    "https://seethemoney.az.gov/Reporting/ExportEntityTransactions/"
                )
                export_params = {
                    "Page": "80",
                    "startYear": str(year),
                    "endYear": str(year),
                    "JurisdictionId": "0|Page",
                    "TablePage": "1",
                    "TableLength": "10",
                    "Name": f"7~{entity_id}",
                    "entityId": str(entity_id),
                    "ChartName": "80",
                    "IsLessActive": "false",
                    "ShowOfficeHolder": "false",
                    "TableSearch": "",
                    "ExportOptions": "CSV",
                }

                try:
                    # Fetch export data
                    export_response = session.get(
                        export_url,
                        params=export_params,
                        headers=AZ_CONTRIBUTIONS_HEADERS,
                    )
                    export_response.raise_for_status()

                    # Get filename from Content-Disposition or use default
                    content_disposition = export_response.headers.get(
                        "Content-Disposition"
                    )
                    if not content_disposition:
                        logger.info("Now Data Found...")
                        continue

                    filename_base = content_disposition.split("filename=")[-1].strip(
                        '"'
                    )
                    clean_name = filename_base.split("_")[0]
                    current_page = url_params["TablePage"]
                    filename = (
                        f"{clean_name}_{entity_id}_{year}_page_{current_page}.csv"
                    )

                    # Save the CSV content
                    output_file = output_dir / filename
                    with open(output_file, "wb") as f:
                        f.write(export_response.content)

                    logger.info(
                        f"Saved contributions data to {output_file}\
                        {year} page {current_page}"
                    )

                    current_page = int(url_params["TablePage"])
                    next_page = current_page + 1
                    url_params["TablePage"] = str(next_page)
                    start = str((next_page - 1) * 10)

                except Exception as e:
                    logger.error(
                        f"Error fetching export data\
                        for EntityID {entity_id}: {e}"
                    )
                    continue

            url_params["TablePage"] = str(int(url_params["TablePage"]) + 1)
            logger.info(
                f"Incrementing TablePage for {year}\
                to {url_params['TablePage']}"
            )

        except Exception as e:
            logger.error(
                f"Error fetching page {url_params['TablePage']}\
                        for {year}: {e}"
            )
            break


@dg.asset(
    deps=[az_fetch_contributions], partitions_def=az_contributions_daily_partition
)
def az_insert_contributions_to_landing(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """Insert Arizona contributions data to the landing table"""
    logger = dg.get_dagster_logger(name="az_insert_contributions_to_landing")
    year = int(context.partition_key)

    # Get all contribution CSV files
    data_dir = Path(AZ_DATA_PATH_PREFIX) / str(year)
    contribution_files = list(data_dir.glob("IndividualAmount_*.csv"))

    if not contribution_files:
        logger.warning("No contribution files found to process")
        return dg.MaterializeResult(metadata={"dagster/row_count": 0})

    table_name = "az_contributions_landing"
    table_name_identifier = sql.Identifier(table_name)
    truncate_query = sql.SQL(
        "DELETE FROM {table_name} WHERE RIGHT(SPLIT_PART(date, ' ', 1), 4) = {year_str}"
    ).format(table_name=table_name_identifier, year_str=sql.Literal(str(year)))

    return az_insert_raw_file_to_landing_table(
        name="contributions",
        postgres_pool=pg.pool,
        table_name=table_name,
        table_columns_name=[
            "date",
            "committee_name",
            "amount",
            "transaction_type",
            "contribution_name",
        ],
        data_files=[
            f"{AZ_DATA_PATH_PREFIX}/{year!s}/{data_file.name!s}"
            for data_file in contribution_files
        ],
        truncate_query=truncate_query,
    )


assets = [
    az_fetch_contributions,
    az_allowed_initial_transactions_insert_to_landing_table,
    az_allowed_offset_transactions_insert_to_landing_table,
    az_committees_insert_to_landing_table,
    az_counties_insert_to_landing_table,
    az_fetch_campaign_finance,
    az_reporting_periods_insert_to_landing_table,
    az_transaction_types_allowed_by_committee_insert_to_landing_table,
    az_transaction_types_insert_to_landing_table,
    az_insert_contributions_to_landing,
]

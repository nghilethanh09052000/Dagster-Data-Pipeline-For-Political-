import csv
import json
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from psycopg import sql

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

NC_BASE_DATA_PATH = "./states/north_carolina"

NC_TRANSACTIONS_TABLE_NAME = "nc_transactions_landing"
NC_TRANSACTIONS_TABLE_COLUMNS = [
    "Name",
    "StreetLine1",
    "StreetLine2",
    "City",
    "State",
    "ZipCode",
    "ProfessionOrJobTitle",
    "EmployersNameOrSpecificField",
    "TransctionType",
    "CommitteeName",
    "CommitteeSBoEID",
    "CommitteeStreet1",
    "CommitteeStreet2",
    "CommitteeCity",
    "CommitteeState",
    "CommitteeZipCode",
    "ReportName",
    "DateOccured",
    "AccountCode",
    "Amount",
    "FormofPayment",
    "Purpose",
    "CandidateOrReferendumName",
    "Declaration",
]

NC_EXPORT_RESULTS_API_URL = "https://cf.ncsbe.gov/CFTxnLkup/ExportResults/"

# NOTE: The data on 1900 still available, although the data comes from reporting data
# from 1990. The only year I was able to find without any data was the first year NC
# was on the 1824 (year they did a presidential election).
NC_FIRST_DATE_DATA_AVAILABLE = datetime(1980, 1, 1)


def nc_get_year_transactions_data_file_path(base_path: Path, year: int) -> Path:
    """
    Get the transaction data file path for a specific year

    Params:
    base_path: the base path of the data file path itself
    year: the year you want to get the path for
    """
    return base_path / f"{year}_transactions.csv"


def nc_get_export_results_payload(start_date: str, end_date: str) -> dict[str, str]:
    """
    Get valid `x-www-form-urlencoded` payload (in object form for requests) for
    North Carolina ExportResults API.

    Params:
    start_date, end_date: valid start and end date, where end date > start date and
                          have the following pattern `MM/DD/YYYY` (stupid date format)
    """

    form_data_params = {
        "ReceiptType": (
            "'GEN','OTLN','IND','PPTY','CPCM','LOAN','RFND','INT','NFPC',"
            "'OUTS','GNS','FRLN','CNRE','LEFO','EPPS','DEBT','DON','BFND'"
        ),
        "ExpenditureType": (
            "'BFND','CCPC','CPE','DEBT','IEXP','INTR','LNRP','NMG','OPER','RFND'"
        ),
        "CommitteeType": "'CNC','JNT','PTY','PAC','REF','IXE','527','IXP'",
        "PartyType": (
            "'CST','DEM','GRN','LIB','NAT','NON','OTH','REP','RFM','SOU','UNA'"
        ),
        "OfficeType": (
            "'ALDR','ATGN','COA','COI','COL','CORO','COUM','CSCT','CTOA',"
            "'CYCM','CYTR','DIAT','DICT','FIRE','GOV','HPTR','LTGV','MAY',"
            "'NCSN','NSHS','OTH','PRES','REGD','RESC','SAUD','SBCY','SBMU',"
            "'SCAJ','SCCJ','SECS','SHER','SNDT','SPCT','STRS','SUPI','SWCS',"
            "'SWSV','TAXC','TCCM','USHS','USSN','VP'"
        ),
        "CommitteeIDs": "",
        "CommitteeName": "",
        "Cities": "",
        "Counties": "",
        "State": "",
        "ZipCodes": "",
        "DateFrom": start_date,
        "DateTo": end_date,
        "OrganizationName": "",
        "FirstName": "",
        "LastName": "",
        "NameSoundsLike": False,
        "NameIsOrg": False,
        "Purpose": "",
        "AmountFrom": "",
        "AmountTo": "",
        "JobProfession": "",
        "JobProfSoundsLike": False,
        "Employer": "",
        "EmployerSoundsLike": False,
        "PaymentType": "",
        "Page": 0,
        "Debug": False,
    }

    return {"Params": json.dumps(form_data_params)}


nc_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=NC_FIRST_DATE_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(partitions_def=nc_yearly_partition)
def nc_transactions(context: dg.AssetExecutionContext):
    """
    This asset will scrape all available transaction data (expenditures and
    contributions) from first available year until the current year.

    The asset will scrape the data one year at a time to make sure that the
    query to their backend doesn't errors out.
    """
    base_path = Path(NC_BASE_DATA_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    year = int(context.partition_key)

    # Get data from 1st of January until 31st of December of the year
    current_year_export_results_payload = nc_get_export_results_payload(
        start_date=f"01/01/{year}", end_date=f"12/31/{year}"
    )
    current_year_file_path = nc_get_year_transactions_data_file_path(
        base_path=base_path, year=year
    )

    context.log.info(
        f"Starting to download data export result on year {year} "
        f"will be saved to {current_year_file_path}"
    )

    with (
        requests.session() as request_session,
        request_session.post(
            NC_EXPORT_RESULTS_API_URL,
            stream=True,
            data=current_year_export_results_payload,
        ) as response,
    ):
        response.raise_for_status()

        response_content_type = response.headers.get("content-type")

        if response_content_type is None or "text/csv" not in response_content_type:
            raise RuntimeError(
                f"Response on year {year} is not `text/csv`! "
                f"Content Type: {response_content_type}"
            )

        with open(current_year_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


@dg.asset(deps=[nc_transactions], partitions_def=nc_yearly_partition)
def nc_insert_transactions_to_landing(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    This asset will insert all of the raw transactions data to landing table
    """
    base_path = Path(NC_BASE_DATA_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    year = int(context.partition_key)

    with (
        pg.pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        context.log.info("Truncating table before inserting new data")
        pg_cursor.execute(
            sql.SQL(
                "delete from {table_name} "
                'where "DateOccured" >= {start_date_str}'
                ' and "DateOccured" <= {end_date_str}'
            ).format(
                table_name=sql.Identifier(NC_TRANSACTIONS_TABLE_NAME),
                start_date_str=f"01/01/{year}",
                end_date_str=f"12/31/{year}",
            )
        )

        current_year_file_path = nc_get_year_transactions_data_file_path(
            base_path=base_path, year=year
        )

        context.log.info(
            f"Starting to insert year {year} data from {current_year_file_path}"
        )

        if not current_year_file_path.is_file():
            context.log.warning(
                f"File from year {year} ({current_year_file_path}) doesn't exists!"
            )
            raise RuntimeError(
                f"File from year {year} ({current_year_file_path}) doesn't exists!"
            )

        data_type_lines_generator = safe_readline_csv_like_file(
            current_year_file_path,
            encoding="utf-8",
        )

        parsed_data_type_file = csv.reader(
            data_type_lines_generator, delimiter=",", quotechar='"'
        )

        next(parsed_data_type_file)  # Skip header

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_data_type_file,
            table_name=NC_TRANSACTIONS_TABLE_NAME,
            table_columns_name=NC_TRANSACTIONS_TABLE_COLUMNS,
            row_validation_callback=lambda row: len(row)
            == len(NC_TRANSACTIONS_TABLE_COLUMNS),
        )

        count_query = get_sql_count_query(table_name=NC_TRANSACTIONS_TABLE_NAME)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        row_count = int(count_cursor_result[0]) if count_cursor_result else 0

        context.log.info(f"Inserted {row_count} rows into {NC_TRANSACTIONS_TABLE_NAME}")

    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": NC_TRANSACTIONS_TABLE_NAME,
            "dagster/row_count": row_count,
        }
    )

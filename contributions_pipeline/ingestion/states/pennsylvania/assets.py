"""
Pennsylvania data consist of contrib, debt, expenses, filer, and receipt.

Data from year full export available (2000) until 2021 all data type have the
same column name and column ordering as well. Though all of the data type
after 2021 all added 2 columns.

The unfortunate part is that all the data only have headers from 2023-2024
onward. Due to all the data type all adding the same number of columns at
the same time, could mean that most of the columns are mostly the same,
and the "old" format could fit on the "new" format without any lost of
meaning.

Glossary:
Data from 2000 - 2021   : "old" format
Data from 2022 - now    : "new" format
"""

import csv
import datetime
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from zipfile import ZipFile

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

PA_DATA_PATH_PREFIX = "./states/pennsylvania"
PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE = 2000

PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE = 2022
PA_LAST_YEAR_OLD_FORMAT_AVAILABLE = PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE - 1


def get_pa_campaign_data_full_export_url(year: int) -> str:
    """
    Get PA campaign finance full export data for a specific year

    Parameters:
    year (int): the year the full export going to be downloaded
    """

    return f"https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/{year}.zip"


def pa_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
    data_file_path: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    last_year_inclusive: int | None = None,
    skip_header: bool = False,
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
    last_year_inclusive: hard limit of year which this insert function will insert the
                         data. If not set, it will use current year instead.
    """

    logger = dg.get_dagster_logger(f"pa_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(PA_DATA_PATH_PREFIX)

    first_year = start_year
    last_year_inclusive = (
        datetime.datetime.now().year
        if last_year_inclusive is None
        else last_year_inclusive
    )
    step_size = 1

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for year in range(first_year, last_year_inclusive + 1, step_size):
            current_cycle_data_file_path = base_path / f"{year}{data_file_path}"

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

                if skip_header:
                    next(parsed_current_cycle_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_current_cycle_file,
                    table_name=table_name,
                    table_columns_name=table_columns_name,
                    row_validation_callback=data_validation_callback,
                )

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
def pa_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Fetch all available full export campaign data from when data available until
    the current year. If any of the year is unavailable it will ignore and log
    a warning.
    """

    data_base_path = Path(PA_DATA_PATH_PREFIX)
    data_base_path.mkdir(parents=True, exist_ok=True)

    first_year = PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE
    last_year_inclusive = datetime.datetime.now().year

    for year in range(first_year, last_year_inclusive + 1):
        campaign_data_full_export_url = get_pa_campaign_data_full_export_url(year=year)
        temp_full_export_zip_file_path = data_base_path / f"{year}.zip"

        try:
            context.log.info(f"Downloading campaign full export data for year {year}")

            stream_download_file_to_path(
                request_url=campaign_data_full_export_url,
                file_save_path=temp_full_export_zip_file_path,
            )

            year_data_folder_path = data_base_path / f"{year}/"
            year_data_folder_path.mkdir(parents=True, exist_ok=True)

            context.log.info(
                f"Exporting campaign full export to {year_data_folder_path}"
            )

            with ZipFile(temp_full_export_zip_file_path, "r") as temp_full_export_zip:
                for file_info in temp_full_export_zip.infolist():
                    cleaned_filename = file_info.filename.split("/")[-1]
                    cleaned_filename = cleaned_filename.split("_")[0]
                    cleaned_filename = cleaned_filename.removesuffix(".txt") + ".txt"

                    target_path = os.path.join(year_data_folder_path, cleaned_filename)

                    # Ignore any folders, flatten all files to the root export path
                    if file_info.filename.endswith("/"):
                        continue

                    # Extract file using buffered streaming
                    with (
                        temp_full_export_zip.open(file_info) as source,
                        open(target_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)  # 64KB buffer
        except RuntimeError as re:
            context.log.warning(f"Failed to process year {year}: {re}")
            continue
        finally:
            temp_full_export_zip_file_path.unlink(missing_ok=True)


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_new_format_contrib_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format PA contrib.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/contrib.txt",
        table_name="pa_contrib_new_format_landing_table",
        # The new format have a header
        skip_header=True,
        table_columns_name=[
            "campaignfinanceid",
            "filerid",
            "eyear",
            "submitteddate",
            "cycle",
            "section",
            "contributor",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "occupation",
            "ename",
            "eaddress1",
            "eaddress2",
            "ecity",
            "estate",
            "ezipcode",
            "contdate1",
            "contamt1",
            "contdate2",
            "contamt2",
            "contdate3",
            "contamt3",
            "contdesc",
        ],
        data_validation_callback=lambda row: len(row) == 26,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_old_format_contrib_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format PA contrib.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        last_year_inclusive=PA_LAST_YEAR_OLD_FORMAT_AVAILABLE,
        data_file_path="/contrib.txt",
        table_name="pennsylvania_old_contrib_landing",
        table_columns_name=[
            "FilerIdentificationNumber",
            "Year",
            "ReportCycleCode",
            "SectionCode",
            "ContributorName",
            "ContributorAddress1",
            "ContributorAddress2",
            "ContributorCity",
            "ContributorState",
            "ContributorZipCode",
            "ContributorOccupation",
            "EmployerName",
            "EmployerAddress1",
            "EmployerAddress2",
            "EmployerCity",
            "EmployerState",
            "EmployerZipCode",
            "ContributionDate1",
            "ContributionAmount1",
            "ContributionDate2",
            "ContributionAmount2",
            "ContributionDate3",
            "ContributionAmount3",
            "ContributionDescription",
        ],
        data_validation_callback=lambda row: len(row) == 24,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_new_format_debt_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format PA debt.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/debt.txt",
        table_name="pa_debt_new_format_landing_table",
        # The new format have a header
        skip_header=True,
        table_columns_name=[
            "campaignfinanceid",
            "filerid",
            "eyear",
            "submitteddate",
            "cycle",
            "dbtname",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "dbtdate",
            "dbtamt",
            "dbtdesc",
        ],
        data_validation_callback=lambda row: len(row) == 14,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_old_format_debt_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format PA debt.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        last_year_inclusive=PA_LAST_YEAR_OLD_FORMAT_AVAILABLE,
        data_file_path="/debt.txt",
        table_name="pennsylvania_old_debt_landing",
        table_columns_name=[
            "FilerIdentificationNumber",
            "Year",
            "ReportCycleCode",
            "CreditorName",
            "CreditorAddress1",
            "CreditorAddress2",
            "CreditorCity",
            "CreditorState",
            "CreditorZipCode",
            "DateDebtIncurred",
            "AmountofDebt",
            "DescriptionofDebt",
        ],
        data_validation_callback=lambda row: len(row) == 12,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_new_format_expense_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format PA expense.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/expense.txt",
        table_name="pa_expense_new_format_landing_table",
        # The new format have a header
        skip_header=True,
        table_columns_name=[
            "campaignfinanceid",
            "filerid",
            "eyear",
            "submitteddate",
            "cycle",
            "expname",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "expdate",
            "expamt",
            "expdesc",
        ],
        data_validation_callback=lambda row: len(row) == 14,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_old_format_expense_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format PA expense.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        last_year_inclusive=PA_LAST_YEAR_OLD_FORMAT_AVAILABLE,
        data_file_path="/expense.txt",
        table_name="pennsylvania_old_expense_landing",
        table_columns_name=[
            "FilerIdentificationNumber",
            "Year",
            "ReportCycleCode",
            "RecipientName",
            "RecipientAddress1",
            "RecipientAddress2",
            "RecipientCity",
            "RecipientState",
            "RecipientZipCode",
            "ExpenditureDate",
            "ExpenditureAmount",
            "ExpenditureDescription",
        ],
        data_validation_callback=lambda row: len(row) == 12,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_new_format_filer_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format PA filer.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/filer.txt",
        table_name="pa_filer_new_format_landing_table",
        # The new format have a header
        skip_header=True,
        table_columns_name=[
            "campaignfinanceid",
            "filerid",
            "eyear",
            "submitteddate",
            "cycle",
            "ammend",
            "terminate",
            "filertype",
            "filername",
            "office",
            "district",
            "party",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "county",
            "phone",
            "beginning",
            "monetary",
            "inkind",
        ],
        data_validation_callback=lambda row: len(row) == 22,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_old_format_filer_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format PA filer.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        last_year_inclusive=PA_LAST_YEAR_OLD_FORMAT_AVAILABLE,
        data_file_path="/filer.txt",
        table_name="pennsylvania_old_filer_landing",
        table_columns_name=[
            "FilerIdentificationNumber",
            "Year",
            "ReportTypeOrCycleCode",
            "Amendment",
            "Termination",
            "FilerTypeCode",
            "FilerName",
            "FilerOffice",
            "FilerDistrict",
            "FilerParty",
            "FilerAddress1",
            "FilerAddress2",
            "FilerCity",
            "FilerState",
            "FilerZipCode",
            "FilerCounty",
            "FilerPhone",
            "BeginningCashBalance",
            "UnitemizedMonetaryContributions",
            "UnitemizedInKindContributions",
        ],
        data_validation_callback=lambda row: len(row) == 20,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_new_format_receipt_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format PA receipt.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/receipt.txt",
        table_name="pa_receipt_new_format_landing_table",
        # The new format have a header
        skip_header=True,
        table_columns_name=[
            "campaignfinanceid",
            "filerid",
            "eyear",
            "submitteddate",
            "cycle",
            "recname",
            "address1",
            "address2",
            "city",
            "state",
            "zipcode",
            "recdesc",
            "recdate",
            "recamt",
        ],
        data_validation_callback=lambda row: len(row) == 14,
    )


@dg.asset(deps=[pa_fetch_all_campaign_data_full_export])
def pa_insert_old_format_receipt_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format PA receipt.txt data to the right landing table"""

    pa_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=PA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        last_year_inclusive=PA_LAST_YEAR_OLD_FORMAT_AVAILABLE,
        data_file_path="/receipt.txt",
        table_name="pennsylvania_old_receipt_landing",
        table_columns_name=[
            "FilerIdentificationNumber",
            "Year",
            "ReportCycleCode",
            "SourceofReceiptName",
            "SourceofReceiptAddress1",
            "SourceofReceiptAddress2",
            "SourceofReceiptCity",
            "SourceofReceiptState",
            "SourceofReceiptZipCode",
            "ReceiptDescription",
            "ReceiptDate",
            "ReceiptAmount",
        ],
        data_validation_callback=lambda row: len(row) == 12,
    )

import calendar
import csv
import os
import shutil
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

MA_DATA_PATH = "./states/massachussets"
MA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE = 2002
MA_FIRST_YEAR_CONTRIBUTION_DATA = datetime(2002, 1, 1)


# Monthly partitions (one partition per month)
ma_contributions_monthly_partition = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 * *",
    fmt="%Y-%m",
    start=MA_FIRST_YEAR_CONTRIBUTION_DATA,
    end_offset=1,
)


def get_url_for_massachussets_bulk_download_one_cycle(
    report_category: str, year: str
) -> str:
    """
    Get massachussets bulk download URL for specific prefix (report type) for a cycle

    Parameters:
    year (int): the year the full export going to be downloaded
    """
    if report_category == "Finance":
        return f"https://ocpf2.blob.core.windows.net/downloads/data2/ocpf-{year}-reports.zip"
    elif report_category == "Committee":
        return "https://ocpf2.blob.core.windows.net/downloads/data2/ocpf-filers.zip"
    else:
        return ""


def ma_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    report_category: str,
    start_year: int,
    data_file_path: str | list[Path],
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    truncate_query: sql.Composed | None = None,
) -> dg.MaterializeResult:
    """
    Insert data files of one report type into the Postgres landing table.
    Handles multiple layouts (Committee, Finance, Contributions).

    Args:
        postgres_pool: Active Postgres connection pool.
        report_category: One of ['Committee', 'Finance', 'contributions'].
        start_year: First available year of the dataset.
        data_file_path: Path or list of paths to the data files.
        table_name: Target Postgres table name.
        table_columns_name: Columns matching file order.
        data_validation_callback: Function to validate parsed rows.
        truncate_query: Optional custom truncate/delete query.
    """

    logger = dg.get_dagster_logger(f"ma_{table_name}_insert")
    base_path = Path(MA_DATA_PATH)

    def _insert_file_to_pg(file_path: Path, cursor):
        """Shared logic for reading, parsing, and inserting one file."""
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}, skipping.")
            return

        logger.info(f"Reading and inserting file: {file_path}")

        try:
            file_lines = safe_readline_csv_like_file(
                file_path=file_path, encoding="utf-8"
            )
            csv_reader = csv.reader(file_lines, delimiter="\t", quotechar='"')
            next(csv_reader, None)  # skip header

            insert_parsed_file_to_landing_table(
                pg_cursor=cursor,
                csv_reader=csv_reader,
                table_name=table_name,
                table_columns_name=table_columns_name,
                row_validation_callback=data_validation_callback,
            )

        except Exception as e:
            logger.error(f"Error while inserting file {file_path}: {e}")
            raise

    # --- Connect and execute insertions ---
    with postgres_pool.connection() as conn, conn.cursor() as cur:
        logger.info(f"Truncating table {table_name} before insert.")
        cur.execute(truncate_query or get_sql_truncate_query(table_name=table_name))

        if report_category == "Committee":
            # Single static file
            file_path = base_path / f"{report_category}{data_file_path}"
            _insert_file_to_pg(file_path, cur)

        elif report_category == "Finance":
            # Yearly iteration
            current_year = datetime.now().year
            for year in range(start_year, current_year + 1):
                file_path = base_path / f"{year}{data_file_path}"
                logger.info(f"Processing Finance data for {year}")
                _insert_file_to_pg(file_path, cur)

        elif report_category == "contributions":
            # List of monthly or ad-hoc files
            file_paths = (
                [Path(p) for p in data_file_path]
                if isinstance(data_file_path, (list, tuple))
                else [Path(data_file_path)]
            )
            for fp in file_paths:
                _insert_file_to_pg(fp, cur)

        else:
            raise ValueError(f"Unknown report_category: {report_category}")

        count_query = get_sql_count_query(table_name=table_name)
        cur.execute(count_query)
        count_result = cur.fetchone()
        row_count = int(count_result[0]) if count_result else 0

        logger.info(f"Inserted {row_count:,} rows into {table_name}.")

        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": table_name,
                "dagster/row_count": row_count,
                "dagster/report_category": report_category,
            }
        )


@dg.asset()
def ma_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext):
    """
    Main function to bulk download all massachussets finance campaign report
    """

    file_path = Path(MA_DATA_PATH)
    file_path.mkdir(parents=True, exist_ok=True)

    first_year = MA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE
    last_year_inclusive = datetime.now().year

    context.log.info("Downloading Committee Candidates full export data.")

    committee_zip_file_path = file_path / "ocpf-filers.zip"
    ocpf_filer_url = get_url_for_massachussets_bulk_download_one_cycle(
        report_category="Committee", year=""
    )

    try:
        stream_download_file_to_path(
            request_url=ocpf_filer_url, file_save_path=committee_zip_file_path
        )

        committee_data_path = file_path / "Committee"
        committee_data_path.mkdir(parents=True, exist_ok=True)

        with ZipFile(committee_zip_file_path, "r") as temp_full_export_zip:
            for file_info in temp_full_export_zip.infolist():
                cleaned_filename = file_info.filename.split("/")[-1]
                cleaned_filename = cleaned_filename.removesuffix(".txt") + ".txt"

                target_path = os.path.join(committee_data_path, cleaned_filename)

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
        context.log.warning(f"Failed to process the data: {re}")
    finally:
        committee_zip_file_path.unlink(missing_ok=True)

    for year in range(first_year, last_year_inclusive + 1):
        context.log.info(
            f"Downloading campaign finance full export data for year {year}"
        )

        temp_full_export_zip_file_path = file_path / f"ocpf-{year}-reports.zip"

        url = get_url_for_massachussets_bulk_download_one_cycle(
            report_category="Finance", year=f"{year}"
        )

        try:
            stream_download_file_to_path(
                request_url=url, file_save_path=temp_full_export_zip_file_path
            )

            year_data_folder_path = file_path / f"{year}/"
            year_data_folder_path.mkdir(parents=True, exist_ok=True)

            with ZipFile(temp_full_export_zip_file_path, "r") as temp_full_export_zip:
                for file_info in temp_full_export_zip.infolist():
                    cleaned_filename = file_info.filename.split("/")[-1]
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


@dg.asset(
    partitions_def=ma_contributions_monthly_partition,
)
def ma_fetch_daily_contributions_data(context: dg.AssetExecutionContext):
    """
    Fetch Massachusetts contribution data via OCPF public API endpoint for each month.
    Each partition loops over every day of that month,
    saving each day's file separately.
    Output file path (e.g., 1_10_2025.txt)
    """

    partition_key = context.partition_key  # e.g. "2025-10"
    year, month = partition_key.split("-")
    year = int(year)
    month = int(month)

    # Get number of days in that month
    _, num_days = calendar.monthrange(year, month)

    # Prepare output folder
    output_dir = Path(MA_DATA_PATH) / "contributions" / str(year) / f"{month:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    context.log.info(
        f"Fetching daily contributions for \
            {calendar.month_name[month]} {year}"
    )

    for day in range(1, num_days + 1):
        start_date_str = f"{month}/{day}/{year}"
        end_date_str = f"{month}/{day}/{year}"
        encoded_start = start_date_str.replace("/", "%2F")
        encoded_end = end_date_str.replace("/", "%2F")

        # Output file path (e.g., 1_10_2025.txt)
        output_file = output_dir / f"{day}_{month}_{year}.txt"

        base_url = (
            "https://api.ocpf.us/search/textOutput"
            f"?searchTypeCategory=A"
            f"&startDate={encoded_start}"
            f"&endDate={encoded_end}"
            "&pagesize=50"
            "&startIndex=1"
            "&sortField="
            "&sortDirection=DESC"
            "&cpfId="
            "&recordTypeId=-1"
            "&name="
            "&cityCode=-1"
            "&state="
            "&zipCode="
            "&occupation="
            "&employer="
            "&minAmount="
            "&maxAmount="
            "&description="
            "&withSummary=true"
        )

        context.log.info(f"Fetching contributions for {start_date_str}")

        try:
            stream_download_file_to_path(
                request_url=base_url, file_save_path=output_file
            )
        except Exception as e:
            context.log.warning(f"Failed to fetch data for {start_date_str}: {e}")
            continue

    context.log.info(
        f"✅ Finished fetching all {num_days} \
        days for {calendar.month_name[month]} {year}"
    )


@dg.asset(deps=[ma_fetch_all_campaign_data_full_export])
def ma_insert_committee_table_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MA all_filers.txt data to the right landing table"""

    ma_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        report_category="Committee",
        start_year=2025,
        data_file_path="/all_filers.txt",
        table_name="ma_committee_candidate_landing_table",
        table_columns_name=[
            "CPF ID",
            "Account Type Code",
            "Comm_Name",
            "Is_Candidate_Only",
            "Candidate First Name",
            "Candidate Last Name",
            "Candidate Street Address",
            "Candidate City",
            "Candidate State",
            "Candidate Zip Code",
            "Treasurer First Name",
            "Treasurer Last Name",
            "Comm Street Address",
            "Comm City",
            "Comm State",
            "Comm Zip Code",
            "Chair First Name",
            "Chair Last Name",
            "Organization Date",
            "District Code Sought",
            "Office Type Sought",
            "District Name Sought",
            "District Code Held",
            "Office Type Held",
            "District Name Held",
            "Related Candidate CPF ID",
            "Closed Date",
            "Party Affiliation",
            "Unknown",
        ],
        data_validation_callback=lambda row: len(row) == 29,
    )


@dg.asset(deps=[ma_fetch_all_campaign_data_full_export])
def ma_insert_report_items_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MA report-items.txt data to the right landing table"""

    ma_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        report_category="Finance",
        start_year=MA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path="/report-items.txt",
        table_name="ma_itemized_contrib_summary_landing_table",
        table_columns_name=[
            "Item_ID",
            "Report_ID",
            "Record_Type_ID",
            "Date",
            "Amount",
            "Name",
            "First_Name",
            "Street_Address",
            "City",
            "State",
            "Zip",
            "Description",
            "Related_CPF_ID",
            "Occupation",
            "Employer",
            "Principal_Officer",
            "Tender_Type_ID",
            "Clarified_Name",
            "Clarified_Purpose",
            "Is_Supported",
            "Is_Previous_Year_Receipt",
            "Unknown",
        ],
        data_validation_callback=lambda row: len(row) == 22,
    )


@dg.asset(deps=[ma_fetch_all_campaign_data_full_export])
def ma_insert_reports_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MA reports.txt data to the right landing table"""

    ma_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        report_category="Finance",
        start_year=MA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path="/reports.txt",
        table_name="ma_non_itemized_contrib_summary_landing_table",
        table_columns_name=[
            "Report_ID",
            "GUID",
            "CPF_ID",
            "Filer_CPF_ID",
            "Report_Type_ID",
            "Report_Type_Description",
            "Is_Amendment",
            "Amendment_To_Report_ID",
            "Filing_Date",
            "Reporting_Period",
            "Report_Year",
            "Start_Date",
            "End_Date",
            "Start_Balance",
            "Receipts_Total",
            "Subtotal",
            "Expenditures_Total",
            "End_Balance",
            "Inkinds_Total",
            "Receipts_Unitemized_Total",
            "Receipts_Itemized_Total",
            "Expenditures_Unitemized_Total",
            "Expenditures_Itemized_Total",
            "Inkinds_Unitemized_Total",
            "Inkinds_Itemized_Total",
            "Liabilities_Itemized_Total",
            "Savings_Total",
            "Out_Of_Pocket_Itemized_Total",
            "Out_Of_Pocket_Unitemized_Total",
            "Deposit_Sequence",
            "Reimbursee",
            "Payments_Made",
            "Interest",
            "Account_Name",
            "Check_Number",
            "OCPF_Candidate_First_Name",
            "OCPF_Candidate_Last_Name",
            "OCPF_Full_Name",
            "OCPF_Depository_Bank_Name",
            "OCPF_District_Code",
            "OCPF_Office",
            "OCPF_District",
            "OCPF_Comm_Name",
            "Payment_Date",
            "In_Transit_Adjustment",
            "Vendor_Name",
            "Unknown",
        ],
        data_validation_callback=lambda row: len(row) == 47,
    )


@dg.asset(
    partitions_def=ma_contributions_monthly_partition,
    deps=[ma_fetch_daily_contributions_data],
)
def ma_insert_contributions_to_landing(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    report_category = "contributions"
    partition_key = context.partition_key  # e.g. "2025-10"
    year, month = partition_key.split("-")
    year = int(year)
    month = int(month)

    table_name = "ma_contributions_landing"
    output_dir = Path(MA_DATA_PATH) / report_category / str(year) / f"{month:02d}"

    truncate_query = sql.SQL(
        """
        DELETE FROM {table}
        WHERE
            EXTRACT(YEAR FROM TO_DATE(date, 'MM/DD/YYYY')) = {year}
            AND EXTRACT(MONTH FROM TO_DATE(date, 'MM/DD/YYYY')) = {month}
        """
    ).format(
        table=sql.Identifier(table_name),
        year=sql.Literal(year),
        month=sql.Literal(month),
    )

    data_file_paths = sorted(output_dir.glob("*.txt"))

    ma_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        report_category=report_category,
        start_year=MA_FIRST_YEAR_CAMPAIGN_DATA_FULL_EXPORT_AVAILABLE,
        data_file_path=data_file_paths,
        table_name="ma_contributions_landing",
        table_columns_name=[
            "record_type_id",
            "record_type_description",
            "filer_cpf_id",
            "filer_full_name_reverse",
            "date",
            "name",
            "first_name",
            "address",
            "city",
            "state",
            "zip_code",
            "amount",
            "occupation",
            "employer",
            "principal_officer",
            "contributor_cpf_id",
            "source_description",
            "description",
            "tender_type_id",
            "tender_type_description",
        ],
        data_validation_callback=lambda row: len(row) == 20,
        truncate_query=truncate_query,
    )

import csv
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
from bs4 import BeautifulSoup, element
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

MO_DATA_PATH = f"{os.getcwd()}/.states/missouri"
STATES_URL = "https://www.mec.mo.gov/MEC/Campaign_Finance/CF_ContrCSV.aspx"
INPUT_ELEMENTS = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE = 2011
MO_LATEST_YEAR_OF_BULK_DATA_AVAILABLE = datetime.now().year + 1

REPORT_TYPES = [
    "CD1_A",
    "CD1_B",
    "CD1A",
    "CD1_C",
    "CD1B1_A",
    "CD3_A",
    "CD3_B",
    "CD3_C",
    "CD1B2_A",
    "summary",
    "CommitteeData",
]


def fetch_data_from_url(
    url: str, report_type: str, year: str | None, downloadables: Path
) -> None:
    session = requests.Session()

    response = session.get(url)

    soup = BeautifulSoup(response.content, "html.parser")
    form_data = {}

    for data in INPUT_ELEMENTS:
        page_el = soup.find("input", {"id": data})
        if isinstance(page_el, element.Tag) and page_el.has_attr("value"):
            form_data.update({data: page_el["value"]})

    rpt_sel_form = "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$ddReportType"
    yr_sel_form = "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$ddReportYear"
    export_btn = "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$btnExport"

    form_data.update(
        {rpt_sel_form: report_type, yr_sel_form: year, export_btn: "Export+to+CSV"}
    )

    response = session.post(url, data=form_data, stream=True)

    with open(downloadables, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def mo_fetch_campaign_data_full_export(
    context: dg.AssetExecutionContext,
    report_type: str,
    year: int | None,
    default_path: Path,
) -> None:
    report_year_msg = ""
    if year is not None:
        report_year_msg = f"\nreport_year: {year}"
    context.log.info(
        (
            "message: Getting Missouri election finance bulk",
            f"download\nreport_type: {report_type}{report_year_msg}",
        )
    )

    while True:
        try:
            fetch_data_from_url(
                url=STATES_URL,
                report_type=report_type,
                year=str(year) if year is not None else year,
                downloadables=default_path,
            )
        except TimeoutError:
            continue

        break


def mo_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int | None,
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

    logger = dg.get_dagster_logger(f"mo_{table_name}_insert")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    base_path = Path(MO_DATA_PATH)

    first_year = start_year
    last_year_inclusive = datetime.now().year
    step_size = 1

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        if first_year is not None:
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
        else:
            try:
                current_cycle_data_file_path = base_path / f"{data_file_path}"

                logger.info(f"Inserting file to pg ({current_cycle_data_file_path})")

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
                logger.warning("File is non-existent, ignoring...")
            except Exception as e:
                logger.error(f"Got error while reading cycle file: {e}")
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
def mo_fetch_all_campaign_data_full_export(context: dg.AssetExecutionContext) -> None:
    """
    Main function to bulk download all missouri finance campaign report, such as

    CD1_A: Itemized Contributions Received - Form CD1 Part A
    CD1_B: Non-Itemized Contributions Received - Form CD1 Part B
    CD1A: Non-Itemized Contributions Received at Fundraisers - Form CD1A
    CD1_C: Loans Received $100 or Less - Form CD1 Part C
    CD1B1_A: Loans Received Over $100 - Form CD1B Part 1
    CD3_A:'Expenditures of $100 or Less by Category - Form CD3 Part A
    CD3_B: Itemized Expenditures Over $100 - Form CD3 Part B
    CD3_C: Contributions Made - Form CD3 Part C
    CD1B2_A: Payments Made or Credits Received on Loans - Form CD1B Part 2
    summary: Report Summary Data
    CommitteeData: Committee Data

    This function will download the csv file.
    """

    for report_type in REPORT_TYPES:
        if report_type != "CommitteeData":
            for year in range(
                MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
                MO_LATEST_YEAR_OF_BULK_DATA_AVAILABLE,
            ):
                context.log.info(
                    (
                        "Starting to download MEC Campaign Finance",
                        f"{year}_{report_type}",
                    )
                )

                default_path = Path(MO_DATA_PATH) / f"{year}/{report_type}"
                default_path.mkdir(parents=True, exist_ok=True)

                mo_fetch_campaign_data_full_export(
                    context=context,
                    report_type=report_type,
                    year=year,
                    default_path=default_path / f"{report_type}.csv",
                )

        else:
            context.log.info(
                ("Starting to download MEC Campaign Finance", f"{report_type}")
            )

            default_path = Path(MO_DATA_PATH) / f"{report_type}"
            default_path.mkdir(parents=True, exist_ok=True)

            mo_fetch_campaign_data_full_export(
                context=context,
                report_type=report_type,
                year=None,
                default_path=default_path / f"{report_type}.csv",
            )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1_a_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1_A.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1_A/CD1_A.csv",
        table_name="mo_cd1_a_landing_table",
        table_columns_name=[
            "CD1_A ID",
            "MECID",
            "Committee Name",
            "Committee",
            "Company",
            "First Name",
            "Last Name",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip",
            "Employer",
            "Occupation",
            "Date",
            "Amount",
            "Contribution Type",
            "Report",
        ],
        data_validation_callback=lambda row: len(row) == 18,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1_b_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1_B.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1_B/CD1_B.csv",
        table_name="mo_cd1_b_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Total Anonymous",
            "Total In-Kind",
            "Total Monetary",
        ],
        data_validation_callback=lambda row: len(row) == 7,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1_c_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1_C.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1_C/CD1_C.csv",
        table_name="mo_cd1_c_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Lender Name",
            "Lender Address1",
            "Lender Address2",
            "Lender City",
            "Lender State",
            "Lender Zip",
            "Date Received",
            "Amount",
        ],
        data_validation_callback=lambda row: len(row) == 12,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1a_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1A.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1A/CD1A.csv",
        table_name="mo_cd1a_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Event Location",
            "Location Address1",
            "Location Address2",
            "Location City",
            "Location State",
            "Location Zip",
            "Event Description",
            "Event Date",
            "Number of Participants",
            "Total Anonymous",
            "Anonymous Explaination",
        ],
        data_validation_callback=lambda row: len(row) == 15,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1b1_a_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1B1_A.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1B1_A/CD1B1_A.csv",
        table_name="mo_cd1b1_a_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Loan RecordID",
            "Lender Name",
            "Lender Address1",
            "Lender Address2",
            "Lender City",
            "Lender State",
            "Lender Zip",
            "Date Received",
            "Loan Amount",
            "Loan ID",
            "Interest Rate",
            "Laon Period",
            "Loan Repayment Schedule",
            "FName Person Liable For Loan",
            "LName Person Liable For Loan",
            "Address1 Person Liable For Loan",
            "Address2 Person Liable For Loan",
            "City Person Liable For Loan",
            "State Person Liable For Loan",
            "Zip Person Liable For Loan",
        ],
        data_validation_callback=lambda row: len(row) == 24,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd1b2_a_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD1B2_A.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD1B2_A/CD1B2_A.csv",
        table_name="mo_cd1b2_a_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Lender",
            "Date",
            "Amount",
            "Type",
            "Payment Method",
        ],
        data_validation_callback=lambda row: len(row) == 9,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd3_a_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD3_A.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD3_A/CD3_A.csv",
        table_name="mo_cd3_a_landing_table",
        table_columns_name=[
            "MECID",
            "Report Name",
            "ReportID",
            "CoverPageID",
            "Expenditure Category",
            "Amount",
            "Expenditure Type",
            "Payment Method",
        ],
        data_validation_callback=lambda row: len(row) == 8,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd3_b_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD3_B.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD3_B/CD3_B.csv",
        table_name="mo_cd3_b_landing_table",
        table_columns_name=[
            "CD3_B ID",
            "MECID",
            "Committee Name",
            "First Name",
            "Last Name",
            "Company",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip",
            "Date",
            "Purpose",
            "Amount",
            "Expenditure Type",
            "Report",
        ],
        data_validation_callback=lambda row: len(row) == 16,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_cd3_c_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CD3_C.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/CD3_C/CD3_C.csv",
        table_name="mo_cd3_c_landing_table",
        table_columns_name=[
            "CD3_C ID",
            "MECID",
            "Committee Name",
            "Committee",
            "Address 1",
            "Address 2",
            "City",
            "State",
            "Zip",
            "Date",
            "Amount",
            "Contribution Type",
            "Report",
        ],
        data_validation_callback=lambda row: len(row) == 13,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_summary_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO summary.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=MO_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        data_file_path="/summary/summary.csv",
        table_name="mo_summary_landing_table",
        table_columns_name=[
            "MECID",
            "Committee Name",
            "ReportID",
            "CoverPageID",
            "Report",
            "Report Year",
            "Report Type",
            "Previous Receipts",
            "Contributions Received",
            "Loans Received",
            "Misc. Receipts",
            "Receipts Subtotal",
            "In-Kind Contributions Received",
            "Total Receipts This Election",
            "Previous Expenditures",
            "Cash or Check Expenditures",
            "In-Kind Expenditures",
            "Credit Expenditures",
            "Expenditure Subtotal",
            "Total Expenditures",
            "Previous Contributions",
            "Cash/Check Contributions",
            "Credit Contributions",
            "In-Kind Contributions Made",
            "Contribution Subtotal",
            "Total Contributions",
            "Loan Disbursements",
            "Disbursements Payments",
            "Misc. Disbursements",
            "Total Disbursements",
            "Starting Money on Hand",
            "Monetary Receipts",
            "Check Disbursements",
            "Cash Disbursements",
            "Total Monetary Disbursements",
            "Ending Money on Hand",
            "Outstanding Indebtedness",
            "Loans Recieved",
            "New Expenditures",
            "New Contributions",
            "Payments Made on Loan",
            "Debt Forgiven on Loans",
            "Total Indebtendness",
        ],
        data_validation_callback=lambda row: len(row) == 43,
    )


@dg.asset(deps=[mo_fetch_all_campaign_data_full_export])
def mo_insert_committee_data_to_landing(pg: dg.ResourceParam[PostgresResource]):
    """Insert new format MO CommitteeData.csv data to the right landing table"""

    mo_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=None,
        data_file_path="CommitteeData/CommitteeData.csv",
        table_name="mo_committee_data_landing_table",
        table_columns_name=[
            "MECID",
            "Committee Type",
            "Committee Name",
            "Committee Status",
            "Acitve Name",
        ],
        data_validation_callback=lambda row: len(row) == 5,
    )

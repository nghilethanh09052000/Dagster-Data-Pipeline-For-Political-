import csv
from pathlib import Path

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

NV_BASE_DATA_PATH = "./states/Nevada"

NV_STORAGE_BASE_API = "https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/nv"


NV_CANDIDATES_FILE_NAME = "CampaignFinance.Cnddt.csv"
NV_GROUPS_FILE_NAME = "CampaignFinance.Grp.csv"
NV_REPORTS_FILE_NAME = "CampaignFinance.Rpr.csv"
NV_CONTRIBUTORS_PAYEES_FILE_NAME = "CampaignFinance.Cntrbtrs.csv"
NV_CONTRIBUTIONS_FILE_NAME = "CampaignFinance.Cntrbt.csv"
NV_EXPENSES_FILE_NAME = "CampaignFinance.Expn.csv"

NV_FILES_LIST = [
    NV_CANDIDATES_FILE_NAME,
    NV_GROUPS_FILE_NAME,
    NV_REPORTS_FILE_NAME,
    NV_CONTRIBUTORS_PAYEES_FILE_NAME,
    NV_CONTRIBUTIONS_FILE_NAME,
    NV_EXPENSES_FILE_NAME,
]


def nv_insert_database_dump_file_to_landing(
    pg_pool: ConnectionPool, file_path: Path, table_name: str, table_columns: list[str]
):
    """
    Insert one type of database dump to the parameters table name and columns.

    Params:
    pg_pool: connection pool from postgres, you can use PostgresResource
    file_path: path to the database dump file (csv)
    table_name: the name of the landing table that the data is going to be inserted
    table_columns: the columns of the landing table ordered the same as the columns
                   of the csv file
    """

    logger = dg.get_dagster_logger(f"nv_insert_to_{table_name}")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        pg_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name}.")
        pg_cursor.execute(query=truncate_query)

        try:
            logger.info(f"Inserting from: {file_path}")

            data_type_lines_generator = safe_readline_csv_like_file(
                file_path,
                encoding="utf-8",
            )

            parsed_data_type_file = csv.reader(
                data_type_lines_generator, delimiter=",", quotechar='"'
            )

            next(parsed_data_type_file)  # Skip header

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_data_type_file,
                table_name=table_name,
                table_columns_name=table_columns,
                row_validation_callback=lambda row: len(table_columns) == len(row),
            )

        except Exception as e:
            logger.error(f"Error while processing {file_path}: {e}")
            raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        row_count = int(count_cursor_result[0]) if count_cursor_result else 0

        logger.info(f"Inserted {row_count} rows into {table_name}")

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset()
def nv_download_campaign_finance_data(context: dg.AssetExecutionContext):
    """
    This asset will download all the campaign finance data as it was extracted from
    Nevada SOS bulk download system. This download will be done through static storage,
    as there's no way to automate the whole request + download just yet.
    """

    base_path = Path(NV_BASE_DATA_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    for file_name in NV_FILES_LIST:
        download_url = NV_STORAGE_BASE_API + f"/{file_name}"
        file_output_path = base_path / file_name

        context.log.info(
            f"Downloading {file_name} ({download_url}) to {file_output_path}"
        )

        stream_download_file_to_path(
            request_url=download_url, file_save_path=file_output_path
        )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_candidates_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert candidates data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_CANDIDATES_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_candidates_landing",
        table_columns=[
            "CandidateID",
            "FirstName",
            "LastName",
            "Party",
            "Office",
            "Jurisdiction",
            "MailingAddress",
            "MailingCity",
            "MailingState",
            "MailingZip",
        ],
    )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_groups_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert groups data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_GROUPS_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_groups_landing",
        table_columns=[
            "GroupID",
            "GroupName",
            "GroupType",
            "ContactName",
            "Active",
            "City",
        ],
    )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_reports_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert reports data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_REPORTS_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_reports_landing",
        table_columns=[
            "ReportID",
            "CandidateID",
            "GroupID",
            "ReportName",
            "ElectionCycle",
            "FilingDueDate",
            "FiledDate",
            "Amended",
            "Superseded",
        ],
    )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_contributors_payees_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert contributors-payees data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_CONTRIBUTORS_PAYEES_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_contributors_payees_landing",
        table_columns=[
            "ContactID",
            "FirstName",
            "MiddleName",
            "LastName",
            "Address1",
            "Address2",
            "City",
            "State",
            "Zip",
        ],
    )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_contribuions_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert contributions data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_CONTRIBUTIONS_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_contributions_landing",
        table_columns=[
            "ContributionID",
            "ReportID",
            "CandidateID",
            "GroupID",
            "ContributionDate",
            "ContributionAmount",
            "ContributionType",
            "ContributorID",
        ],
    )


@dg.asset(deps=[nv_download_campaign_finance_data])
def nv_insert_expenses_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert expenses data from Nevada database dump
    """

    base_path = Path(NV_BASE_DATA_PATH)
    file_path = base_path / NV_EXPENSES_FILE_NAME

    nv_insert_database_dump_file_to_landing(
        pg_pool=pg.pool,
        file_path=file_path,
        table_name="nv_expenses_landing",
        table_columns=[
            "ExpenseID",
            "ReportID",
            "CandidateID",
            "GroupID",
            "ExpenseDate",
            "ExpenseAmount",
            "ExpenseType",
            "PayeeID",
        ],
    )

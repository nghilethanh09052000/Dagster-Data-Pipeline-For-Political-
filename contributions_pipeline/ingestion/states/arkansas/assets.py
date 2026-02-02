import csv
import os
from collections.abc import Callable
from datetime import datetime
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

AR_POST_REQUEST_GET_RAW_DATA = (
    "https://api-ethics-disclosures.sos.arkansas.gov/api/ExportData/GetExportData"
)
AR_DATA_PATH_PREFIX = "./states/arkansas"
AR_FIRST_YEAR_DATA_AVAILABLE = datetime(year=2001, month=1, day=1)
AR_DATASETS = [
    {
        "name": "candidates",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "filerTypeCode": "CAN",
            "filerName": "",
            "politicalPartyCode": "",
            "OfficeSought": "",
            "totalRaisedMax": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "balanceFundsMax": None,
            "balanceFundsMin": None,
            "accountStatus": "FACT",
            "election": "",
            "transactionSourceTypeCode": None,
        },
        "columns": [
            "filer_name",
            "office",
            "office_district_name",
            "filer_entity_version_id",
            "filer_entity_id",
            "filer_type",
            "political_party",
            "filer_status",
            "total_raised",
            "total_spent",
            "balance_of_funds",
            "election_year",
            "guid",
            "filer_type_code",
            "filer_type_desc",
            "filing_year",
            "committee_name",
            "first_name",
            "last_name",
            "suffix",
            "balance_of_funds_new",
            "total_raised_new",
            "total_spent_new",
            "filing_type_code",
            "is_paper_filer",
            "total_rows",
        ],
    },
    {
        "name": "commitees",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "filerTypeCode": "COM",
            "filerName": "",
            "politicalPartyCode": "",
            "OfficeSought": "",
            "totalRaisedMax": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "balanceFundsMax": None,
            "balanceFundsMin": None,
            "committeeType": "",
            "filingYear": "",
            "accountStatus": "FACT",
        },
        "columns": [
            "filerName",
            "office",
            "officeDistrictName",
            "filerEntityVersionID",
            "filerEntityID",
            "filerType",
            "politicalParty",
            "filerStatus",
            "totalRaised",
            "totalSpent",
            "balanceofFunds",
            "electionYear",
            "guid",
            "filerTypeCode",
            "filerTypeDesc",
            "filingYear",
            "committeeName",
            "firstName",
            "lastName",
            "suffix",
            "balanceofFundsNew",
            "totalRaisedNew",
            "totalSpentNew",
            "filingTypeCode",
            "isPaperFiler",
            "totalRows",
        ],
    },
    {
        "name": "expenditures",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicTransactionDetails/GetTransactionDetails"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "transactionTypeCode": "TEXP",
            "sourceTypeCode": None,
            "committeeType": None,
            "transactionSubTypeCode": None,
            "electionID": None,
            "sourceAddress": None,
            "toDate": None,
            "fromDate": None,
            "reportName": None,
            "city": None,
            "state": None,
            "address": None,
            "transactionCategory": None,
        },
        "columns": [
            "guid",
            "filer_name",
            "transaction_amount",
            "transaction_date",
            "source_name",
            "employer_name",
            "occupation",
            "source_address",
            "transaction_source",
            "report_name",
            "transaction_sub_type_desc",
            "has_child",
            "filer_registration_guid",
            "transaction_category",
            "total_rows",
        ],
    },
    {
        "name": "loans_debts",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicTransactionDetails/GetPublicLoansAndDebtsDetails"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "transactionAmount": 0,
            "transactionAmountBy": "",
            "toDate": None,
            "fromDate": None,
            "committeeType": None,
            "electionID": None,
            "sourceTypeCode": "",
            "byState": "",
            "transactionSubType": None,
            "transactionTypeCode": None,
            "reportName": None,
        },
        "columns": [
            "filer_guid",
            "transaction_id",
            "filer_name",
            "filer_entity_id",
            "filer_registration_id",
            "filer_registration_guid",
            "transaction_amount",
            "balance_amount",
            "transaction_date",
            "source_name",
            "employer_name",
            "occupation",
            "source_address",
            "transaction_source",
            "transaction_type_code",
            "transaction_type_desc",
            "report_name",
            "guarantor",
            "page_number",
            "page_size",
            "guid",
            "transaction_sub_type_desc",
            "filer_type_code",
            "filer_type_desc",
            "has_child",
            "new_transaction_amount",
            "new_balance_amount",
            "new_transaction_date",
            "total_rows",
        ],
    },
    {
        "name": "fillings_and_reports",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFiledReportAndDownload/GetPublicFilingReport"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "filerType": None,
            "officeSought": None,
            "reportType": None,
            "reportStatus": None,
            "filedDateFromDate": None,
            "filedDateToDate": None,
            "reportName": None,
            "electionID": None,
            "electionYear": None,
        },
        "columns": [
            "report_name",
            "file_path",
            "start_date",
            "end_date",
            "due_date",
            "filed_date",
            "filer_name",
            "filer_type",
            "office_name",
            "report_type",
            "report_status",
            "filer_entity_id",
            "filer_entity_vers_id",
            "filer_report_guid",
            "filer_filing_calendar_guid",
            "filer_registration_guid",
            "report_version",
            "filer_report_version_id",
            "has_child",
            "is_paper_file",
            "total_rows",
        ],
    },
    {
        "name": "legacy_filled_reports",
        "api_url": (
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicLegacyFiledReport/GetPublicLegacyFiledReport"
        ),
        "payload": {
            "pageNumber": 1,
            "pageSize": 100000,
            "filerName": "",
            "filerTypeCode": "",
            "reportType": "",
            "fileName": "",
            "officeType": "",
            "reportStatus": "",
            "filedDateFromDate": None,
            "filedDateToDate": None,
            "filedDate": None,
            "electionID": "",
            "registrationYear": "",
            "moduleName": "CF",
        },
        "columns": [
            "filer_name",
            "file_name",
            "report_type",
            "filer_type",
            "office_type",
            "registration_year",
            "filed_date",
            "filer_registration_guid",
            "legacy_filer_registration_id",
            "total_rows",
            "filer_type_code",
            "max_filed_date",
            "min_filed_date",
            "report_start_date",
            "report_end_date",
            "report_due_date",
            "report_version",
            "filer_report_guid",
            "id",
            "file_path",
        ],
    },
]


def ar_insert_data_to_landing_table(
    postgres_pool: ConnectionPool,
    dataset_name: str,
    table_name: str,
    columns: list[str],
    data_validation_callback: Callable,
    data_file: str,
    truncate_query: sql.Composed | None = None,
):
    """
    Insert raw ar-ACCESS data to landing table in Postgres.

    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.

    dataset_name: the DATASETS mapping we use to check to get the correct file path

    table_name: the name of the table that's going to be used for ingesting

    columns: list of columns name in order of the data files that's going to
                        be inserted. For example for
                        campaign_finance_reporting_history_landing.
                        ["report_number","amends_report","amended_by_report",
                         "report_data", ..., "attachments"]

    data_validation_callback: callable to validate the cleaned rows, if the function
                            return is false, the row will be ignored, a warning
                            will be shown on the logs

    data_file: the file path of the actual data file on the Arkansas folder,
                        so that we will have corrent path and use
                        it to ingest to table
    """

    logger = dg.get_dagster_logger(name=f"wa_{dataset_name}_insert")

    if not os.path.exists(data_file):
        logger.error(f"Data file {data_file} does not exist.")
        return dg.MaterializeResult(
            metadata={"dagster/error": "Data file does not exist."}
        )

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

        logger.info(f"Processing file: {data_file}")
        data_type_lines_generator = safe_readline_csv_like_file(
            data_file, encoding="utf-8"
        )

        # Read as tab-separated values
        parsed_data_type_file = csv.reader(data_type_lines_generator, delimiter=",")

        # Skip the header
        next(parsed_data_type_file, None)

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_data_type_file,
            table_name=table_name,
            table_columns_name=columns,
            row_validation_callback=data_validation_callback,
        )

        # Verify row count
        count_query = get_sql_count_query(table_name=table_name)

        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(f"Successfully inserted {row_count} rows into {table_name}")
        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


def ar_fetch_raw_data(
    dataset_name: str,
    api_url: str,
    payload: dict,
    year: int | None = None,
):
    """
    Fetch raw data for Arkansas campaign finance datasets on
    this webiste https://ethics-disclosures.sos.arkansas.gov/public/cf

    1. Make a POST request to determine total items.
    2. Use the total items to update PageSize in the data request payload.
    3. Make a POST request to fetch the actual data.

    Args:
        dataset_name (str): Name of the dataset.
        api_url (str): Endpoint suffix for total items request.
        total_item_request_payload (dict): Payload for the total items request.
        data_request_payload (dict): Payload for the actual data request.

    Returns:
        dict: The fetched data.
    """

    logger = dg.get_dagster_logger(name=f"ar_{dataset_name}_fetch")
    page_number = 1
    total_items_url = api_url
    results = []
    while True:
        payload["pageNumber"] = page_number
        response = requests.post(total_items_url, json=payload)
        response.raise_for_status()
        data = response.json()
        items = data.get("data", {}).get("items", [])

        if not items:
            break

        results.extend(items)

        logger.info(f"Fetched {len(items)} records from page {page_number}")
        page_number += 1

    if not results:
        logger.warning(f"No data found for {dataset_name}.")
        return None

    Path(AR_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)

    filename_prefix = f"{year!s}_" if year else ""
    csv_filename = f"{AR_DATA_PATH_PREFIX}/{filename_prefix}{dataset_name}.csv"

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"Dataset {dataset_name} saved to {csv_filename}")


def ar_create_fetch_raw_data_assets(dataset: dict):
    @dg.asset(
        name=f"ar_{dataset.get('name')}_fetch_raw_data",
    )
    def fetch_data():
        return ar_fetch_raw_data(
            dataset_name=dataset["name"],
            api_url=dataset["api_url"],
            payload=dataset["payload"],
        )

    return fetch_data


def ar_create_insert_to_landing_table_assets(dataset):
    fetch_asset_name = f"ar_{dataset.get('name')}_fetch_raw_data"

    @dg.asset(
        deps=[fetch_asset_name],
        name=f"ar_{dataset.get('name')}_insert_data_to_landing_table",
    )
    def insert_data(pg: dg.ResourceParam[PostgresResource]):
        dataset_name = dataset["name"]
        table_name = f"ar_{dataset['name']}_landing"
        columns = dataset["columns"]
        data_file = f"{AR_DATA_PATH_PREFIX}/{dataset_name}.csv"

        return ar_insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name=dataset,
            table_name=table_name,
            columns=columns,
            data_validation_callback=lambda row: len(row) == len(columns),
            data_file=data_file,
        )

    return insert_data


ar_contributions_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=AR_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)


@dg.asset(
    name="ar_fetch_contributions_data",
    partitions_def=ar_contributions_yearly_partition,
    description="Partition for Arkansas contributions data, partitioned by year.",
)
def ar_fetch_contributions_data(context: dg.AssetExecutionContext):
    start_date = context.partition_time_window.start.replace(month=1, day=1)
    end_date = context.partition_time_window.start.replace(month=12, day=30)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    context.log.info(f"Fetching data from {start_date_str} to {end_date_str}")

    return ar_fetch_raw_data(
        dataset_name="contributions",
        api_url=(
            "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicTransactionDetails/GetTransactionDetails"
        ),
        payload={
            "pageNumber": 1,
            "pageSize": 100000,
            "transactionTypeCode": "TCON",
            "sourceTypeCode": None,
            "committeeType": None,
            "transactionSubTypeCode": None,
            "electionID": None,
            "reportName": None,
            "toDate": end_date_str,
            "fromDate": start_date_str,
            "byState": None,
        },
        year=int(context.partition_key),
    )


@dg.asset(
    deps=[ar_fetch_contributions_data],
    partitions_def=ar_contributions_yearly_partition,
)
def ar_contributions_insert_data_to_landing_table(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert raw contributions data to landing table in Postgres.
    """
    logger = dg.get_dagster_logger(name="ar_insert_contributions_to_landing")
    year = int(context.partition_key)

    logger.info(f"Inserting data for year {year} into ar_contributions_landing")

    return ar_insert_data_to_landing_table(
        postgres_pool=pg.pool,
        dataset_name="contributions",
        table_name="ar_contributions_landing",
        columns=[
            "guid",
            "filer_name",
            "transaction_amount",
            "transaction_date",
            "source_name",
            "employer_name",
            "occupation",
            "source_address",
            "transaction_source",
            "report_name",
            "transaction_sub_type_desc",
            "has_child",
            "filer_registration_guid",
            "transaction_category",
            "total_rows",
        ],
        data_validation_callback=lambda row: len(row) == 15,
        data_file=f"{AR_DATA_PATH_PREFIX}/{year}_contributions.csv",
        truncate_query=sql.SQL(
            "DELETE FROM ar_contributions_landing "
            "WHERE RIGHT(transaction_date, 4) = {year_str}"
        ).format(year_str=sql.Literal(str(year))),
    )


fetch_assets = [ar_create_fetch_raw_data_assets(dataset) for dataset in AR_DATASETS]

insert_assets = [
    ar_create_insert_to_landing_table_assets(dataset) for dataset in AR_DATASETS
]

assets = (
    fetch_assets
    + insert_assets
    + [ar_fetch_contributions_data]
    + [ar_contributions_insert_data_to_landing_table]
)

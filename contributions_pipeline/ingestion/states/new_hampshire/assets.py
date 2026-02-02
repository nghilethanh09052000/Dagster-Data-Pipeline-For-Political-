import csv
import typing
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

NH_PROXIES: dict[str, str] = {
    "http": typing.cast(str, dg.EnvVar("NH_HTTP_PROXY").get_value(default="")),
    "https": typing.cast(str, dg.EnvVar("NH_HTTPS_PROXY").get_value(default="")),
}

NH_DATA_PATH_PREFIX = "./states/new_hampshire"

NH_FIRST_YEAR_DATA_AVAILABLE = datetime(2016, 1, 1)

NH_ELECTION_LOOK_UP_API_URL = (
    "https://cfsapi.sos.nh.gov/api/Lookup/GetElectionLookupData"
)

NH_BULK_DATA_TEXP_TCON_API_URL = (
    "https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData"
)
NH_BULK_DATA_CANDIDATES_API_URL = (
    "https://cfsapi.sos.nh.gov/api/PublicGridDownload/DownloadPublicGridData"
)

NH_BULK_DATA_PAC_API_URL = "https://cfsapi.sos.nh.gov/api/ExportData/GetExportData"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-length": "379",
    "content-type": "application/json",
    "origin": "https://cfs.sos.nh.gov",
    "priority": "u=1, i",
    "referer": "https://cfs.sos.nh.gov/",
    "sec-ch-ua": ('"Chromium";v="134", "Not:A-Brand";v="24", "Microsoft Edge";v="134"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    ),
}


def nh_insert_data_to_landing_table(
    postgres_pool: ConnectionPool,
    dataset_name: str,
    table_name: str,
    columns: list[str],
    data_validation_callback: Callable,
    data_file: str,
    truncate: bool = False,
    truncate_query: sql.Composed | None = None,
    transform_row_callback: Callable[[list[str]], list[str]] | None = None,
):
    """
    Insert raw nh-ACCESS data to landing table in Postgres.

    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource pnhams.
                     You can add `pg: dg.ResourcePnham[PostgresResource]` to your asset
                     pnhameters to get accees to the resource.

    dataset_name: the DATASETS mapping we use to check to get the correct file path

    table_name: the name of the table that's going to be used for ingesting

    columns: list of columns name in order of the data files that's going to
                        be inserted. For example for
                        campaign_finance_reporting_history_landing.
                        ["report_number","amends_report","amended_by_report",
                         "report_data", ..., "attachments"]

    data_validation_callback: callable to validate the cleaned rows, if the function
                            return is False, the row will be ignored, a wnhning
                            will be shown on the logs

    data_file: the file path of the actual data file on the Arkansas folder,
                        so that we will have corrent path and use
                        it to ingest to table

    truncate: should the function truncate the table
    """

    logger = dg.get_dagster_logger(name=f"wa_{dataset_name}_insert")
    final_truncate_query = (
        get_sql_truncate_query(table_name=table_name)
        if truncate_query is None
        else truncate_query
    )

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        if truncate:
            logger.info(f"Truncating table {table_name} before inserting new data.")
            pg_cursor.execute(final_truncate_query)

        logger.info(f"Processing file: {data_file}")
        data_type_lines_generator = safe_readline_csv_like_file(
            data_file, encoding="utf-8"
        )

        # Read as tab-sepnhated values
        parsed_data_type_file = csv.reader(
            data_type_lines_generator, delimiter=",", quotechar='"'
        )

        # Skip the header
        next(parsed_data_type_file, None)

        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_data_type_file,
            table_name=table_name,
            table_columns_name=columns,
            row_validation_callback=data_validation_callback,
            transform_row_callback=transform_row_callback,
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


nh_contributions_yearly_partition = dg.TimeWindowPartitionsDefinition(
    # Partition for year
    cron_schedule="0 0 1 1 *",
    # Make each of the partition to be yearly
    fmt="%Y",
    start=NH_FIRST_YEAR_DATA_AVAILABLE,
    end_offset=1,
)


def nh_get_election_cycle_lookup_data() -> str:
    r = requests.post(
        url=NH_ELECTION_LOOK_UP_API_URL,
        headers=HEADERS,
        json={"key": "", "isSelectReq": False, "name": "filingCycle"},
        proxies=NH_PROXIES,
    )
    r.raise_for_status()
    data = r.json()
    results = data.get("data")
    results = [str(result.get("value")) for result in results]
    return ",".join(results)


def nh_fetch_bulk_candidate_data_for_data_type_on_a_year(
    url: str, payload: dict[str, str], data_type: str
):
    """
    Generic helper to download NH campaign finance CSV and save it to disk.
    """
    logger = dg.get_dagster_logger(name=f"nh_{data_type}_download")
    base_path = Path(NH_DATA_PATH_PREFIX)
    base_path.mkdir(parents=True, exist_ok=True)

    save_file_path = base_path / f"{data_type}.csv"

    response = requests.post(
        url=url, headers=HEADERS, json=payload, proxies=NH_PROXIES, stream=True
    )
    response.raise_for_status()
    logger.info(f"Saving {data_type} to {save_file_path}...")
    with open(save_file_path, "wb") as save_file:
        for chunk in response.iter_content(chunk_size=8192):
            save_file.write(chunk)


def nh_fetch_bulk_exp_con_data_for_data_type_on_a_year(
    data_type: typing.Literal["TCON", "TEXP"], year: int
):
    """
    This function will use the configured proxy to start download on NH Campaign Finance
    system bulk data for the specific year and data type.

    This function will save the saved file to
    {NH_DATA_PATH_PREFIX}/{year}_{data_type}.csv.

    Params:
    data_type: either "TCON" for receipts, or "TEXP" for expenditure data
    year: the year of data that will be downloaded
    """
    logger = dg.get_dagster_logger(name=f"nh_{year}_{data_type}_download")

    base_path = Path(NH_DATA_PATH_PREFIX)
    base_path.mkdir(parents=True, exist_ok=True)

    save_file_path = base_path / f"{year}_{data_type}.csv"

    with (
        requests.session() as session,
        session.post(
            NH_BULK_DATA_TEXP_TCON_API_URL,
            headers=HEADERS,
            json={
                "type": "CSV",
                "filingYear": str(year),
                "transactionTypeCode": data_type,
            },
            stream=True,
            proxies=NH_PROXIES,
        ) as download_response,
    ):
        download_response.raise_for_status()

        logger.info(f"Saving {data_type} ({year}) to {save_file_path}...")
        with open(save_file_path, "wb") as save_file:
            for chunk in download_response.iter_content(chunk_size=8192):
                save_file.write(chunk)


@dg.asset(pool="nh_api")
def nh_fetch_candidate():
    lookup_data = nh_get_election_cycle_lookup_data()
    payload = {
        "publicGridName": "FilingEntitiesPublicGrid",
        "candidateCommitteeSearchFilter": {
            "pageNumber": 1,
            "pageSize": 10,
            "sortBy": "FilerName",
            "sortType": "asc",
            "filerTypeCode": "CAN",
            "filerSearchTypeCode": "CAN",
            "filerSubTypeCode": None,
            "filerName": None,
            "politicalPartyCode": None,
            "officeSought": None,
            "totalRaisedMax": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "accountStatus": "FACT",
            "officeType": None,
            "electionCycle": lookup_data,
            "county": None,
            "CommitteeMakingIE": "",
        },
        "type": "CSV",
        "openInNewTab": False,
    }
    return nh_fetch_bulk_candidate_data_for_data_type_on_a_year(
        url=NH_BULK_DATA_CANDIDATES_API_URL, payload=payload, data_type="CAN"
    )


@dg.asset(pool="nh_api")
def nh_fetch_candidate_committee():
    lookup_data = nh_get_election_cycle_lookup_data()
    payload = {
        "publicGridName": "FilingEntitiesPublicGrid",
        "candidateCommitteeSearchFilter": {
            "pageNumber": 1,
            "pageSize": 10,
            "sortBy": "FilerName",
            "sortType": "asc",
            "filerTypeCode": "CAN",
            "filerSearchTypeCode": "CC",
            "filerSubTypeCode": None,
            "filerName": None,
            "politicalPartyCode": None,
            "officeSought": None,
            "totalRaisedMax": None,
            "totalRaisedMin": None,
            "totalSpentMax": None,
            "totalSpentMin": None,
            "accountStatus": "FACT",
            "officeType": None,
            "electionCycle": lookup_data,
            "county": None,
            "CommitteeMakingIE": "",
        },
        "type": "CSV",
        "openInNewTab": False,
    }
    return nh_fetch_bulk_candidate_data_for_data_type_on_a_year(
        url=NH_BULK_DATA_CANDIDATES_API_URL, payload=payload, data_type="CC"
    )


@dg.asset(pool="nh_api")
def nh_fetch_political_committee(context: dg.AssetExecutionContext):
    lookup_data = nh_get_election_cycle_lookup_data()

    payload = {
        "referenceProcedure": "public-committee-export",
        "parameters": [
            {"key": "@FilerTypeCode", "value": "COM"},
            {"key": "@FilerName", "value": ""},
            {"key": "@FilerSearchTypeCode", "value": ""},
            {"key": "@FilerSubTypeCode", "value": ""},
            {"key": "@Election", "value": ""},
            {"key": "@AccountStatus", "value": "FACT"},
            {"key": "@CommitteeMakingIE", "value": ""},
            {"key": "@TotalRaisedMin", "value": None},
            {"key": "@TotalRaisedMax", "value": None},
            {"key": "@TotalSpentMin", "value": None},
            {"key": "@TotalSpentMax", "value": None},
            {"key": "@BalanceFundsMin ", "value": None},
            {"key": "@BalanceFundsMax", "value": None},
            {"key": "@ElectionCycle", "value": lookup_data},
            {"key": "@FilingTypeCode", "value": None},
            {"key": "@PageNumber", "value": 1},
            {"key": "@PageSize", "value": 1204},
            {"key": "@SortBy"},
            {"key": "@SortType"},
        ],
        "dataColumnName": [
            "FilerEntityID",
            "CandidateCommitteeName",
            "RegistrationType",
            "FilerSubtypeName",
            "CommitteePurpose",
            "TaxExemptFlagStatus",
            "CommitteeAddressLine1",
            "CommitteeAddressLine2",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteePhoneNumber",
            "CommitteeEmail",
            "CommitteeWebsite",
            "ChairpersonFirstName",
            "ChairpersonLastName",
            "TreasurerFirstName",
            "TreasurerLastName",
            "CommitteeMakingIE",
            "ElectionCycle",
            "TotalRaisedNew",
            "TotalSpentNew",
            "FilerStatus",
        ],
        "displayColumnName": [
            "Filing Entity Id",
            "Committee Name",
            "Registration Type",
            "Committee Subtype",
            "Committee Purpose",
            "Exempt from Taxation under 501",
            "Committee Address Line 1",
            "Committee Address Line 2",
            "Committee City",
            "Committee State",
            "Committee Zip Code",
            "Committee Phone Number",
            "Committee Email",
            "Committee Website",
            "Chairperson First Name",
            "Chairperson Last Name",
            "Treasurer First Name",
            "Treasurer Last Name",
            "Indicated Making Independent Expenditures",
            "Election Cycle",
            "Total Receipts",
            "Total Expenditures",
            "Status",
        ],
        "type": "CSV",
        "fileName": "NonCandidateCommitteData09292025",
        "isHeaderRequired": True,
        "headerTitle": "Non-Candidate Committee Data Download as of",
        "openInNewTab": False,
    }

    return nh_fetch_bulk_candidate_data_for_data_type_on_a_year(
        url=NH_BULK_DATA_PAC_API_URL, payload=payload, data_type="COM"
    )


@dg.asset(partitions_def=nh_contributions_yearly_partition, pool="nh_api")
def nh_fetch_expenditures(context: dg.AssetExecutionContext):
    """
    Download New Hampshire expenditure bulk data
    """

    nh_fetch_bulk_exp_con_data_for_data_type_on_a_year(
        data_type="TEXP", year=int(context.partition_key)
    )


@dg.asset(partitions_def=nh_contributions_yearly_partition, pool="nh_api")
def nh_fetch_receipts(context: dg.AssetExecutionContext):
    """
    Download New Hampshire receipts bulk data
    """

    nh_fetch_bulk_exp_con_data_for_data_type_on_a_year(
        data_type="TCON", year=int(context.partition_key)
    )


@dg.asset(deps=[nh_fetch_candidate])
def nh_insert_candidate(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert NH candidates data
    """
    base_path = Path(NH_DATA_PATH_PREFIX)
    file_path = base_path / "CAN.csv"

    nh_insert_data_to_landing_table(
        postgres_pool=pg.pool,
        dataset_name="candidate",
        table_name="nh_candidate_landing",
        columns=[
            "filerEntityId",
            "registrationType",
            "candidateFirstName",
            "candidateLastName",
            "candidateCommitteeName",
            "filerSubtypeName",
            "committeeName",
            "committeePurpose",
            "taxExemptFlagStatus",
            "officeType",
            "office",
            "county",
            "officeDistrictName",
            "politicalParty",
            "electionYear",
            "electionCycle",
            "totalRaisedNew",
            "totalSpentNew",
            "committeeMakingIe",
            "candidateMailingAddressLine1",
            "candidateMailingAddressLine2",
            "candidateMailingAddressCity",
            "candidateMailingAddressState",
            "candidateMailingAddressZipCode",
            "candidatePhone",
            "candidateEmail",
            "candidateWebsite",
            "committeeAddressLine1",
            "committeeAddressLine2",
            "committeeCity",
            "committeeState",
            "committeeZipCode",
            "committeePhoneNumber",
            "committeeEmail",
            "committeeWebsite",
            "chairpersonLastName",
            "chairpersonFirstName",
            "treasurerLastName",
            "treasurerFirstName",
            "filerStatus",
        ],
        data_file=str(file_path.absolute()),
        data_validation_callback=lambda row: len(row) == 40,
        truncate=True,
    )


@dg.asset(deps=[nh_fetch_candidate_committee])
def nh_insert_candidate_committee(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert NH candidate committee data
    """
    base_path = Path(NH_DATA_PATH_PREFIX)
    file_path = base_path / "CC.csv"

    nh_insert_data_to_landing_table(
        postgres_pool=pg.pool,
        dataset_name="candidate_committee",
        table_name="nh_candidate_committee_landing",
        columns=[
            "filerEntityId",
            "registrationType",
            "candidateFirstName",
            "candidateLastName",
            "candidateCommitteeName",
            "filerSubtypeName",
            "committeeName",
            "committeePurpose",
            "taxExemptFlagStatus",
            "officeType",
            "office",
            "county",
            "officeDistrictName",
            "politicalParty",
            "electionYear",
            "electionCycle",
            "totalRaisedNew",
            "totalSpentNew",
            "committeeMakingIe",
            "candidateMailingAddressLine1",
            "candidateMailingAddressLine2",
            "candidateMailingAddressCity",
            "candidateMailingAddressState",
            "candidateMailingAddressZipCode",
            "candidatePhone",
            "candidateEmail",
            "candidateWebsite",
            "committeeAddressLine1",
            "committeeAddressLine2",
            "committeeCity",
            "committeeState",
            "committeeZipCode",
            "committeePhoneNumber",
            "committeeEmail",
            "committeeWebsite",
            "chairpersonLastName",
            "chairpersonFirstName",
            "treasurerLastName",
            "treasurerFirstName",
            "filerStatus",
        ],
        data_file=str(file_path.absolute()),
        data_validation_callback=lambda row: len(row) == 40,
        truncate=True,
    )


@dg.asset(deps=[nh_fetch_political_committee])
def nh_insert_political_committee(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert NH committee data
    """
    base_path = Path(NH_DATA_PATH_PREFIX)
    file_path = base_path / "COM.csv"
    nh_insert_data_to_landing_table(
        postgres_pool=pg.pool,
        dataset_name="political_committee",
        table_name="nh_political_committee_landing",
        columns=[
            "filingEntityId",
            "committeeName",
            "registrationType",
            "committeeSubtype",
            "committeePurpose",
            "exemptFromTaxationUnder501",
            "committeeAddressLine1",
            "committeeAddressLine2",
            "committeeCity",
            "committeeState",
            "committeeZipCode",
            "committeePhoneNumber",
            "committeeEmail",
            "committeeWebsite",
            "chairpersonFirstName",
            "chairpersonLastName",
            "treasurerFirstName",
            "treasurerLastName",
            "indicatedMakingIndependentExpenditures",
            "electionCycle",
            "totalReceipts",
            "totalExpenditures",
            "status",
        ],
        data_file=str(file_path.absolute()),
        data_validation_callback=lambda row: len(row) == 23,
        truncate=True,
    )


@dg.asset(
    deps=[nh_fetch_expenditures], partitions_def=nh_contributions_yearly_partition
)
def nh_insert_expenditures(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert NH expenditures data
    """
    base_path = Path(NH_DATA_PATH_PREFIX)
    year = context.partition_key

    file_path = base_path / f"{year}_TEXP.csv"

    if file_path.exists() and file_path.is_file():
        nh_insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name="new_expenditures",
            table_name="nh_new_expenditures_landing",
            columns=[
                "FilingEntityID",
                "FilingEntityName",
                "FilingEntityType",
                "TransactionType",
                "TransactionSubType",
                "PayeeOrWorkerOrCreditorOrLoansourcetype",
                "PayeeOrWorkerOrCreditorOrLoanSourceName",
                "PayeeOrWorkerOrCreditorOrLoansourceAddress",
                "TransactionAmount",
                "TransactionDate",
                "ElectionType",
                "TransactionDescription",
                "TimedReport",
                "PartitionKey",
            ],
            data_file=str(file_path.absolute()),
            data_validation_callback=lambda row: len(row) == 13,
            transform_row_callback=lambda row: [*row, context.partition_key],
            truncate=True,
            truncate_query=sql.SQL(
                "DELETE FROM nh_new_expenditures_landing"
                ' WHERE "PartitionKey" = {partition_key}'
            ).format(partition_key=context.partition_key),
        )

    else:
        context.log.warning(f"File {file_path} not found, ignoring...")


@dg.asset(deps=[nh_fetch_receipts], partitions_def=nh_contributions_yearly_partition)
def nh_insert_receipts(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert NH receipts data
    """
    base_path = Path(NH_DATA_PATH_PREFIX)
    year = context.partition_key

    file_path = base_path / f"{year}_TCON.csv"

    if file_path.exists() and file_path.is_file():
        nh_insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name="new_receipts",
            table_name="nh_new_receipts_landing",
            columns=[
                "FilingEntityID",
                "CandidateName",
                "CommitteeName",
                "CommitteeSubtype",
                "TransactionType",
                "TransactionSubType",
                "ElectionPeriod",
                "Electionyear",
                "DateofReceipt",
                "Amountofreceipt",
                "ContributorType",
                "ContributorName",
                "ContributorAddressLine1",
                "ContributorAddressLine2",
                "ContributorCity",
                "ContributorState",
                "ContributorZipCode",
                "Contributoroccupation",
                "ContributorEmployer",
                "ContributorPrincipleplaceofBusiness",
                "Description",
                "TimedReport",
                "PartitionKey",
            ],
            data_file=str(file_path.absolute()),
            data_validation_callback=lambda row: len(row) == 22,
            transform_row_callback=lambda row: [*row, context.partition_key],
            truncate=True,
            truncate_query=sql.SQL(
                "DELETE FROM nh_new_receipts_landing"
                ' WHERE "PartitionKey" = {partition_key}'
            ).format(partition_key=context.partition_key),
        )
    else:
        context.log.warning(f"File {file_path} not found, ignoring...")


assets = [
    nh_fetch_candidate,
    nh_fetch_candidate_committee,
    nh_fetch_political_committee,
    nh_fetch_expenditures,
    nh_fetch_receipts,
    nh_insert_candidate,
    nh_insert_candidate_committee,
    nh_insert_political_committee,
    nh_insert_expenditures,
    nh_insert_receipts,
]

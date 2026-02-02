"""
Virgina Ingestion Assets

# Transitional Schema Case

Most of the reporting here follows the timeline below.
The timeline also usually correlates with the folder structure.

1999 - 2011 : "old" tables (e.g. `scheduleb_old_landing`),
              with folders using `{year}` format.
2012 - now  : "new" tables (`scheduleb_new_landing`),
              with folders using `{year}_{month}` format.

There's special case for Schedule A PAC, where
out of nowhere there's a transitional period of Schema changes. Thus,
the timeline is more like so.

1999 - 2003 : "old" tables
2004 - 2011 : "transitional" table
2012 - now  : "new" table

Even more bizzare, Schedule D PAC revert back in the middle back to the
"old" schema. So the schema looks something like so.

1999 - 2003 : "old" tables
2004 - 2007 : "transitional" table
2008 - 2008 : reverted back to "old" table
2009 - 2009 : reverted back to "transitional" table
2010 - 2010 : reverted back to "old" table
2011 - 2011 : reverted back to "transitional" table
2012 - now  : "new" table


Same thing happened to Schedule D report type, just with different
timeline of Schema changes.

1999 - 2001 : "old" tables
2001 - 2011 : "transitional" table
2012 - now  : "new" table
"""

import csv
import datetime
import logging
import os
import time
from collections.abc import Callable, Generator
from pathlib import Path

import dagster as dg
import requests
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import (
    fetch_url_with_chrome_user_agent,
    get_all_links_from_html,
    save_response_as_file,
)
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE = 1999
VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE = 2019

# This "transitional" format is a weird format change
# for Schedule A PAC
VA_FIRST_YEAR_TRANSITIONAL_SCHEDULE_A_PAC_FORMAT_AVAILABLE = 2004
VA_LAST_YEAR_BEFORE_TRANSITIONAL_SCHEDULE_A_FORMAT_AVAILABLE = (
    VA_FIRST_YEAR_TRANSITIONAL_SCHEDULE_A_PAC_FORMAT_AVAILABLE - 1
)

# Schedule D PAC have quite a unique format, thus it's represented as a timeline array
# of array. Where each item in the array reprsented the start year, and the end year
VA_SCHEDULE_D_PAC_OLD_TABLE_TIMELINE = [
    [VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE, 2003],
    [2008, 2008],
    [2010, 2010],
]
VA_SCHEDULE_D_PAC_TRANSITIONAL_TABLE_TIMELINE = [
    [2004, 2007],
    [2009, 2009],
    [2011, 2011],
]

# Schedule D have quite a unique format, thus it's represented as a timeline array
# of array. Where each item in the array reprsented the start year, and the end year
VA_SCHEDULE_D_OLD_TABLE_TIMELINE = [
    [VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE, 2000],
    [2002, 2002],
]
VA_SCHEDULE_D_TRANSITIONAL_TABLE_TIMELINE = [
    [2001, 2001],
    [2003, 2011],
    [2011, 2011],
]

VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE = 2012
VA_FIRST_YEAR_ZEROTH_MONTH_DATA = 2020

VA_LAST_YEAR_OF_OLD_FORMAT = VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE - 1
VA_LAST_YEAR_OF_TRANSITIONAL_FORMAT = VA_LAST_YEAR_OF_OLD_FORMAT


def get_result_directory_path_for_va_one_year_bulk_download(year_month: str) -> str:
    """
    Get the result directory for a specific year_month (could be year or year_month
    format depending if year is under 2012 or after). Hint: use
    `va_get_year_month_generator` for easier iteration.

    Parameters
    ----------
        year_month: (str)
                    string with "year" or "year_month" format, e.g. "1999", "2020_01",
                    "2003_12", etc.

    """

    return f"./states/virgnia/{year_month}"


def va_fetch_all_files_of_all_report_type_all_time(
    base_url: str, report_type: str
) -> None:
    """
    Main function to bulk download all virginia finance campaign report. This
    function will download the csv file.

    Parameters
    ----------
        base_url: (string)
                  base url to get virgnia campaign finance report
        report_type: (string)
    """

    logger = logging.getLogger(va_fetch_all_files_of_all_report_type_all_time.__name__)

    base_page_request = fetch_url_with_chrome_user_agent(url=base_url)

    if not base_page_request.ok:
        raise RuntimeError(
            (
                f"Failed to fetch base page: {base_url}",
                f"response code: {base_page_request.status_code}",
                f"response body: {base_page_request.text}",
                f"response header: {base_page_request.headers}",
            )
        )

    base_page = base_page_request.text
    download_urls = get_all_links_from_html(page_source=base_page)

    logger.info(
        (
            f"Got sub-page links from base page: {download_urls}",
            f"Raw Page: {base_page}",
        )
    )

    csv_files = []
    for url in download_urls:
        logger.info(f"Getting more url for {url}")
        download_page_request = fetch_url_with_chrome_user_agent(url=url)

        if not download_page_request.ok:
            raise RuntimeError(
                (
                    f"Failed to fetch download page {url}",
                    f"response code: {download_page_request.status_code}",
                    f"response body: {download_page_request.text}",
                    f"response header: {base_page_request.headers}",
                )
            )

        download_page = download_page_request.text
        all_file_link_on_download_page = get_all_links_from_html(
            page_source=download_page
        )

        logger.info(
            (
                f"Got file links from sub-page ({url}):",
                all_file_link_on_download_page,
                f"Raw Page: {download_page}",
            )
        )

        csv_files.extend(all_file_link_on_download_page)

        time.sleep(3)

    for link in csv_files:
        report_year_month, file_name = link.split("/")[-2:]

        logger.info(
            {
                "message": "Getting Virginia election finance bulk download",
                "url": link,
                "report_year": report_year_month,
            }
        )

        report_year_month_dir = get_result_directory_path_for_va_one_year_bulk_download(
            year_month=report_year_month
        )
        Path(report_year_month_dir).mkdir(parents=True, exist_ok=True)

        if "%20" in file_name:
            file_name = file_name.replace("%20", "")

        report_file_path = f"{report_year_month_dir}/{file_name}"

        if not os.path.exists(report_file_path):
            response = fetch_url_with_chrome_user_agent(link)
            while response.status_code != 200:
                time.sleep(1)
                response = fetch_url_with_chrome_user_agent(link)

            save_response_as_file(
                response=response, destination_file_path=report_file_path
            )
        else:
            logger.info(
                ("File already exists", report_file_path),
            )

        time.sleep(3)


def va_get_year_month_generator(
    start_year: int, end_year: int | None = None
) -> Generator[str]:
    """
    Creates a generator that follows the VA before and after 2012 year or year_month
    folder formatting. The generator will create a generator until the the current
    year and month.

    If `end_year` and `end_month` is None, then it will go until current year and month.

    Parameters
    ----------
        start_year: (int)
                    the year the genetaror going to generate (inclusive)
        end_year: (int)
                  the last year (inclusive) the generator should stop

    """

    last_old_format_exclusive = VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE
    for year in range(start_year, last_old_format_exclusive):
        if end_year is not None and year > end_year:
            return

        yield str(year)

    current_datetime = datetime.datetime.now()
    last_year_inclusive = current_datetime.year
    last_month_of_year_inclusive = current_datetime.month

    for year in range(last_old_format_exclusive, last_year_inclusive + 1):
        start_month = 1

        if year == VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE:
            start_month = 3
        elif year >= VA_FIRST_YEAR_ZEROTH_MONTH_DATA:
            start_month = 0

        end_month_inclusive = (
            last_month_of_year_inclusive if year == last_year_inclusive else 12
        )

        for month in range(start_month, end_month_inclusive + 1):
            if end_year is not None and year > end_year:
                return

            yield f"{year}_{month:02d}"


def va_fetch_all_files_from_storage(
    base_url: str, file_names: list[str], start_year: int
):
    """
    This funciton is going to dowload all the files from a static download site.
    The expected URL format is "{base_url}/{year(_month)}/{either of file_names}".
    If there's 404 on any of the file_name, the function will only logs it,
    no error will be raised.

    Params:
    base_url: base url of the storage
    file_names: list of file names on the year that you want to download,
                e.g. ["ScheduleA.csv", "ScheduleF_PAC.csv", ...]
    start_year: the first year you want to check for all of the file names in the
                base url, this will end in the current year and month
    """

    logger = dg.get_dagster_logger("va_fetch_from_storage")

    last_year_inclusive = datetime.datetime.now().year

    for year_month in va_get_year_month_generator(
        start_year=start_year, end_year=last_year_inclusive
    ):
        year_data_base_path = Path(
            get_result_directory_path_for_va_one_year_bulk_download(
                year_month=year_month
            )
        )
        year_data_base_path.mkdir(parents=True, exist_ok=True)

        with requests.session() as req_session:
            for file_name in file_names:
                file_download_uri = f"{base_url}/{year_month}/{file_name}"

                logger.info(
                    f"Downloading {file_name} on year {year_month} "
                    f"({file_download_uri})"
                )

                with req_session.get(file_download_uri, stream=True) as response:
                    if response.status_code == 404 or response.status_code == 400:
                        logger.warning(
                            f"File {file_name} on year {year_month} "
                            f"not found (URL: {file_download_uri}"
                        )
                        continue

                    response.raise_for_status()

                    file_save_path = year_data_base_path / file_name
                    logger.info(
                        f"Saving {file_name} on year {year_month} to {file_save_path}"
                    )

                    with open(file_save_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)


def va_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
    data_file_path: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
    end_year: int | None = None,
    truncate: bool = False,
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
    start_year: the first year of when the data is available.
    data_file_path: the file path of the actual data file on the year folder, for
                    example individual contributions, the local folder for it is laid
                    out as it follows (year_month) -> ScheduleA.csv. The data file is
                    ScheduleA.csv, thus the input should be "/ScheduleA.csv".
    table_name: the name of the table that's going to be used. for example
                `schedulea_new_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for individual contributions.
                        ["ReportId", "CommitteeContactId", "FirstName", "MiddleName",
                         "LastOrCompanyName", ..., "ReportUID"]
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    end_year: the last inclusive year the data will be loaded
    """

    logger = dg.get_dagster_logger(f"va_insert_{data_file_path}")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        if truncate:
            logger.info("Truncating table before inserting the new one")
            pg_cursor.execute(query=truncate_query)

        for year_month in va_get_year_month_generator(
            start_year=start_year, end_year=end_year
        ):
            base_path = get_result_directory_path_for_va_one_year_bulk_download(
                year_month=year_month
            )
            current_cycle_data_file_path = base_path + data_file_path

            logger.info(
                (
                    f"Inserting year {year_month} file to pg",
                    f"({current_cycle_data_file_path})",
                )
            )

            try:
                current_year_file_lines_generator = safe_readline_csv_like_file(
                    file_path=current_cycle_data_file_path,
                    encoding="utf-8",
                )
                parsed_current_year_file = csv.reader(
                    current_year_file_lines_generator,
                    delimiter=",",
                    quoting=csv.QUOTE_MINIMAL,
                )

                # Skip the header
                next(parsed_current_year_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_current_year_file,
                    table_name=table_name,
                    table_columns_name=table_columns_name,
                    row_validation_callback=data_validation_callback,
                )

            except FileNotFoundError:
                logger.warning(
                    f"File for year {year_month} is non-existent, ignoring..."
                )
            except Exception as e:
                logger.error(
                    f"Got error while reading cycle year {year_month} file: {e}"
                )
                raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()

        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        return dg.MaterializeResult(
            metadata={"dagster/table_name": table_name, "dagster/row_count": row_count}
        )


@dg.asset(
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=30,  # 30s delay from each retry
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    )
)
def fetch_virginia_campaign_finance():
    """
    Virginia campaign election finance report required by
    the Virginia Department of Elections.
    """
    # NOTE: as VA errors out when downloaded automatically through Dagster, the data
    # have been mirroed to Supabase Storage
    va_fetch_all_files_from_storage(
        base_url="https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/va",
        file_names=[
            "ScheduleA_PAC.csv",
            "ScheduleA.csv",
            "ScheduleB_PAC.csv",
            "ScheduleB.csv",
            "ScheduleC_PAC.csv",
            "ScheduleC.csv",
            "ScheduleD_PAC.csv",
            "ScheduleD.csv",
            "ScheduleE_PAC.csv",
            "ScheduleE.csv",
            "ScheduleF_PAC.csv",
            "ScheduleF.csv",
            "ScheduleG_PAC.csv",
            "ScheduleG.csv",
            "ScheduleH_PAC.csv",
            "ScheduleH.csv",
            "ScheduleI_PAC.csv",
            "ScheduleI.csv",
            "Report.csv",
            "CandidateCampaignCommittee.csv",
            "InauguralCommittee.csv",
            "OutOfStatePoliticalActionCommittee.csv",
            "PoliticalActionCommittee.csv",
            "PoliticalPartyCommittee.csv",
            "ReferendumCommittee.csv",
        ],
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_candidate_campaign_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert Candidate Campaign Committee data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/CandidateCampaignCommittee.csv",
        table_name="va_candidate_campaign_committee_landing",
        table_columns_name=[
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "OfficeType",
            "LocalityName",
            "DistrictName",
            "OfficeSoughtName",
            "ElectionName",
            "ElectionDate",
            "PoliticalPartyName",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebsite",
            "VoterRegistrationid",
            "CandidateSalutation",
            "CandidateFirstName",
            "CandidateMiddleName",
            "CandidateLastName",
            "CandidateSuffix",
            "CandidateStreetAddress",
            "CandidateSuiteNumber",
            "CandidateCity",
            "CandidateState",
            "CandidateZipCode",
            "CandidateEmailAddress",
            "CandidateDayTimePhoneNumber",
            "TreasurerVoterId",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "FilingMethod",
            "ApprovedVendor",
            "SubmittedOn",
            "AcceptedOn",
            "CandidateCountyOrCityOfResidence",
            "TreasurerCountyOrCityOfResidence",
            "CandidateIsRegisteredToVote",
            "TreasurerIsRegisteredToVote",
            "DateFirstContributionAccepted",
            "DateFirstExpenditureMade",
            "DateCampaignDepositoryDesignated",
            "DateTreasurerAppointed",
            "DateStatementOfQualificationFiled",
            "DateFilingFeePaidForPartyNomination",
        ],
        data_validation_callback=lambda row: len(row) == 59,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_inagural_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert Inagural Committee data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/InauguralCommittee.csv",
        table_name="va_inagural_committee_landing",
        table_columns_name=[
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebsite",
            "ElectedOfficialSalutation",
            "ElectedOfficialFirstName",
            "ElectedOfficialMiddleName",
            "ElectedOfficialLastName",
            "ElectedOfficialSuffix",
            "ElectedOfficialStreetAddress",
            "ElectedOfficialSuite",
            "ElectedOfficialCity",
            "ElectedOfficialState",
            "ElectedOfficialZipCode",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerBusinessStreetAddress",
            "TreasurerBusinessSuite",
            "TreasurerBusinessCity",
            "TreasurerBusinessState",
            "TreasurerBusinessZipCode",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "CustodianPositionOrTitle",
            "CustodianSalutation",
            "CustodianFirstName",
            "CustodianMiddleName",
            "CustodianLastName",
            "CustodianSuffix",
            "CustodianBusinessAddress",
            "CustodianBusinessSuiteNumber",
            "CustodianBusinessCity",
            "CustodianBusinessState",
            "CustodianBusinessZipCode",
            "CustodianStreetAddress",
            "CustodianSuite",
            "CustodianCity",
            "CustodianState",
            "CustodianZipCode",
            "CustodianEmailAddress",
            "CustodianDayTimePhoneNumber",
            "AddressMaintainStreetAddress",
            "AddressMaintainSuite",
            "AddressMaintainCity",
            "AddressMaintainState",
            "AddressMaintainZipCode",
            "FilingMethod",
            "ApprovedVendor",
            "ElectedOfficeName",
            "OtherOfficeName",
            "DateFirstContributionAccepted",
            "DateCommitteeOrganized",
            "DateCampaignDepositoryDesignated",
            "DateTreasurerAppointed",
            "DateFirstExpenditureMade",
            "DateStatementOfQualificationFiled",
        ],
        data_validation_callback=lambda row: len(row) == 72,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_out_of_state_political_action_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert out of state PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/OutOfStatePoliticalActionCommittee.csv",
        table_name="va_out_of_state_political_action_committee_landing",
        table_columns_name=[
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "TaxPayerIdentificationNumber",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebsite",
            "PurposeOfCommittee",
            "AffiliatedOrganizationName",
            "AffiliatedStreetAddress",
            "AffiliatedSuite",
            "AffiliatedCity",
            "AffiliatedState",
            "AffiliatedZipCode",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerBusinessStreetAddress",
            "TreasurerBusinessSuite",
            "TreasurerBusinessCity",
            "TreasurerBusinessState",
            "TreasurerBusinessZipCode",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "CustodianSalutation",
            "CustodianFirstName",
            "CustodianMiddleName",
            "CustodianLastName",
            "CustodianSuffix",
            "CustodianBusinessAddress",
            "CustodianBusinessSuiteNumber",
            "CustodianBusinessCity",
            "CustodianBusinessState",
            "CustodianBusinessZipCode",
            "CustodianStreetAddress",
            "CustodianSuite",
            "CustodianCity",
            "CustodianState",
            "CustodianZipCode",
            "AddressMaintainStreetAddress",
            "AddressMaintainSuite",
            "AddressMaintainCity",
            "AddressMaintainState",
            "AddressMaintainZipCode",
            "FilingMethod",
            "ApprovedVendor",
            "CustodianEmail",
            "CustodianDayTimePhoneNumber",
            "StatewideElections",
            "GeneralAssemblyElections",
            "LocalElections",
            "LocalElection1",
            "LocalElection2",
            "LocalElection3",
            "LocalElection4",
            "LocalElection5",
            "LocalElection6",
            "CommitteeAgencyName",
        ],
        data_validation_callback=lambda row: len(row) == 71,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_political_action_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/PoliticalActionCommittee.csv",
        table_name="va_political_action_committee_landing",
        table_columns_name=[
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "CommitteeAcronym",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebsite",
            "PurposeOfCommittee",
            "AffiliatedOrganizationName",
            "AffiliatedStreetAddress",
            "AffiliatedSuite",
            "AffiliatedCity",
            "AffiliatedState",
            "AffiliatedZipCode",
            "RelationshipToOrganization",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerBusinessStreetAddress",
            "TreasurerBusinessSuite",
            "TreasurerBusinessCity",
            "TreasurerBusinessState",
            "TreasurerBusinessZipCode",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "CustodianPositionOrTitle",
            "CustodianSalutation",
            "CustodianFirstName",
            "CustodianMiddleName",
            "CustodianLastName",
            "CustodianSuffix",
            "CustodianBusinessAddress",
            "CustodianBusinessSuiteNumber",
            "CustodianBusinessCity",
            "CustodianBusinessState",
            "CustodianBusinessZipCode",
            "CustodianEmail",
            "CustodianDayTimePhoneNumber",
            "CustodianStreetAddress",
            "CustodianSuite",
            "CustodianCity",
            "CustodianState",
            "CustodianZipCode",
            "AddressMaintainStreetAddress",
            "AddressMaintainSuite",
            "AddressMaintainCity",
            "AddressMaintainState",
            "AddressMaintainZipCode",
            "FilingMethod",
            "ApprovedVendor",
            "SubmittedOn",
            "AcceptedOn",
            "DateExpendituresExceeded200",
            "StatewideElections",
            "GeneralAssemblyElections",
            "LocalElections",
            "LocalElection1",
            "LocalElection2",
            "LocalElection3",
            "LocalElection4",
            "LocalElection5",
            "LocalElection6",
            "Officer1Name",
            "Officer1Title",
            "Officer1DayTimePhone",
            "Officer2Name",
            "Officer2Title",
            "Officer2DayTimePhone",
            "DateContributionsExceeded200",
            "DateTreasurerAppointed",
            "DateCampaignDepositoryDesignated",
        ],
        data_validation_callback=lambda row: len(row) == 84,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_political_party_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert political party committee data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/PoliticalPartyCommittee.csv",
        table_name="va_political_party_committee_landing",
        table_columns_name=[
            "PartyAffiliation",
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebSite",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerBusinessAddress",
            "TreasurerBusinessSuiteNumber",
            "TreasurerBusinessCity",
            "TreasurerBusinessState",
            "TreasurerBusinessZipCode",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "CustodianPositionOrTitle",
            "CustodianSalutation",
            "CustodianFirstName",
            "CustodianMiddleName",
            "CustodianLastName",
            "CustodianSuffix",
            "CustodianBusinessAddress",
            "CustodianBusinessSuiteNumber",
            "CustodianBusinessCity",
            "CustodianBusinessState",
            "CustodianBusinessZipCode",
            "CustodianStreetAddress",
            "CustodianSuite",
            "CustodianCity",
            "CustodianState",
            "CustodianZipCode",
            "AddressMaintainStreetAddress",
            "AddressMaintainSuite",
            "AddressMaintainCity",
            "AddressMaintainState",
            "AddressMaintainZipCode",
            "FilingMethod",
            "ApprovedVendor",
            "SubmittedOn",
            "AcceptedOn",
            "DistrictName",
            "CustodianEmail",
            "CustodianDayTimePhoneNumber",
            "DateFirstContributionAccepted",
            "DateFirstExpenditureMade",
            "DateTreasurerAppointed",
            "DateCampaignDepositoryDesignated",
            "CommitteeScope",
            "LocalityName",
            "Officer1Name",
            "Officer1Title",
            "Officer1DayTimePhone",
            "Officer2Name",
            "Officer2Title",
            "Officer2DayTimePhone",
        ],
        data_validation_callback=lambda row: len(row) == 70,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_referendum_committee_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """Insert referendum committee data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_COMMITTEE_DATA_AVAILABLE,
        data_file_path="/ReferendumCommittee.csv",
        table_name="va_referendum_committee_landing",
        table_columns_name=[
            "CommitteeCode",
            "CommitteeName",
            "IsAmendment",
            "DateChangesTookEffect",
            "CommitteeAcronym",
            "CommitteeStreetAddress",
            "CommitteeSuite",
            "CommitteeCity",
            "CommitteeState",
            "CommitteeZipCode",
            "CommitteeEmailAddress",
            "CommitteePhone",
            "CommitteeWebsite",
            "AffiliatedOrganizationName",
            "AffiliatedStreetAddress",
            "AffiliatedSuite",
            "AffiliatedCity",
            "AffiliatedState",
            "AffiliatedZipCode",
            "RelationshipToOrganization",
            "TreasurerSalutation",
            "TreasurerFirstName",
            "TreasurerMiddleName",
            "TreasurerLastName",
            "TreasurerSuffix",
            "TreasurerBusinessStreetAddress",
            "TreasurerBusinessSuite",
            "TreasurerBusinessCity",
            "TreasurerBusinessState",
            "TreasurerBusinessZipCode",
            "TreasurerStreetAddress",
            "TreasurerSuite",
            "TreasurerCity",
            "TreasurerState",
            "TreasurerZipCode",
            "TreasurerEmail",
            "TreasurerDayTimePhoneNumber",
            "CustodianPositionOrTitle",
            "CustodianSalutation",
            "CustodianFirstName",
            "CustodianMiddleName",
            "CustodianLastName",
            "CustodianSuffix",
            "CustodianBusinessAddress",
            "CustodianBusinessSuiteNumber",
            "CustodianBusinessCity",
            "CustodianBusinessState",
            "CustodianBusinessZipCode",
            "CustodianStreetAddress",
            "CustodianSuite",
            "CustodianCity",
            "CustodianState",
            "CustodianZipCode",
            "AddressMaintainStreetAddress",
            "AddressMaintainSuite",
            "AddressMaintainCity",
            "AddressMaintainState",
            "AddressMaintainZipCode",
            "FilingMethod",
            "ApprovedVendor",
            "SubmittedOn",
            "AcceptedOn",
            "ReferendumDate",
            "CustodianEmail",
            "CustodianDayTimePhoneNumber",
            "DateFirstContributionAccepted",
            "DateFirstExpenditureMade",
            "DateTreasurerAppointed",
            "DateCampaignDepositoryDesignated",
            "IsLocal",
            "IsStatewide",
            "IsRegional",
            "ReferendumLocation",
            "PositionOfReferendum",
            "ReferendumSubject",
        ],
        data_validation_callback=lambda row: len(row) == 75,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_a_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule A (contributions received) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_BEFORE_TRANSITIONAL_SCHEDULE_A_FORMAT_AVAILABLE,
        data_file_path="/ScheduleA_PAC.csv",
        table_name="schedulea_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "entity_employer",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
        ],
        data_validation_callback=lambda row: len(row) == 28,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_transitional_schedule_a_pac_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert VA transitional schema schedule A (contributions received) by PACs data to
    landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_TRANSITIONAL_SCHEDULE_A_PAC_FORMAT_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_TRANSITIONAL_FORMAT,
        data_file_path="/ScheduleA_PAC.csv",
        table_name="schedulea_pac_transitional_landing",
        table_columns_name=[
            "committee_code",
            "committee_id",
            "committee_type_code",
            "committee_name",
            "report_year",
            "date_received",
            "date_acknowledged",
            "filing_status",
            "filing_type",
            "election_year",
            "officer_position",
            "lastname",
            "firstname",
            "middlename",
            "sched_id",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amnt",
            "trans_agg_to_date",
            "entity_employer",
            "report_due_date",
            "report_code",
            "report_date",
            "report_ending_date",
            "filing_source_type",
            "amended_count",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_a_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule A (contributions received) data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleA.csv",
        table_name="schedulea_new_landing",
        table_columns_name=[
            "reportid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "nameofemployer",
            "occupationortypeofbusiness",
            "primarycityandstateofemploymentorbusiness",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "totaltodate",
            "scheduleaid",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 22,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_b_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule B (expenditures) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleB_PAC.csv",
        table_name="scheduleb_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 30,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_b_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA old format schedule B (expenditures) by candidates data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleB.csv",
        table_name="scheduleb_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_b_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule B (expenditures) data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleB.csv",
        table_name="scheduleb_new_landing",
        table_columns_name=[
            "reportid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "nameofemployer",
            "occupationortypeofbusiness",
            "primarycityandstateofemploymentorbusiness",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "totaltodate",
            "schedulebid",
            "valuationbasis",
            "productorservice",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 24,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_c_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule C (loans) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleC_PAC.csv",
        table_name="schedulec_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 30,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_c_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA old format schedule C (loans) by committe data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleC.csv",
        table_name="schedulec_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_c_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule C (loans) data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleC.csv",
        table_name="schedulec_new_landing",
        table_columns_name=[
            "reportid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "schedulecid",
            "receipttype",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 19,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_d_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule D (debts and obligations) by PACs data to landing table"""

    should_truncate = True
    last_metadata = None

    for [start_year, end_year] in VA_SCHEDULE_D_PAC_OLD_TABLE_TIMELINE:
        last_metadata = va_insert_files_to_landing_of_one_type(
            postgres_pool=pg.pool,
            start_year=start_year,
            end_year=end_year,
            data_file_path="/ScheduleD_PAC.csv",
            table_name="scheduled_pac_old_landing",
            table_columns_name=[
                "committee_code",
                "committee_name",
                "report_year",
                "report_due_date",
                "report_code",
                "report_start_date",
                "report_ending_date",
                "fling_source_type",
                "filing_type",
                "amended_count",
                "date_received",
                "filing_status",
                "officer_position",
                "last_name",
                "first_name",
                "middle_name",
                "entity_name",
                "entity_address",
                "entity_city",
                "entity_state",
                "entity_zip",
                "entity_occupation",
                "entity_place_of_business",
                "trans_type",
                "trans_date",
                "trans_amount",
                "trans_agg_to_date",
                "trans_value_basis",
                "trans_service_or_goods",
                "entity_employer",
            ],
            data_validation_callback=lambda row: len(row) == 30,
            truncate=should_truncate,
        )

        should_truncate = False

    return last_metadata


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_transitional_schedule_d_pac_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert VA transitional schema schedule D (debts and obligations) by PACs data to
    landing table
    """

    should_truncate = True
    last_metadata = None

    for [start_year, end_year] in VA_SCHEDULE_D_PAC_TRANSITIONAL_TABLE_TIMELINE:
        last_metadata = va_insert_files_to_landing_of_one_type(
            postgres_pool=pg.pool,
            start_year=start_year,
            end_year=end_year,
            data_file_path="/ScheduleD_PAC.csv",
            table_name="scheduled_pac_transitional_landing",
            table_columns_name=[
                "committee_code",
                "committee_id",
                "committee_type_code",
                "committee_name",
                "report_year",
                "date_received",
                "date_acknowledged",
                "filing_status",
                "filing_type",
                "election_year",
                "officer_position",
                "lastname",
                "firstname",
                "middlename",
                "sched_id",
                "entity_name",
                "entity_address",
                "entity_city",
                "entity_state",
                "entity_zip",
                "entity_occupation",
                "entity_place_of_business",
                "trans_type",
                "trans_date",
                "trans_amnt",
                "trans_officer",
                "trans_item_or_service",
                "sch_record_key",
                "report_due_date",
                "report_code",
                "report_date",
                "report_ending_date",
                "filing_source_type",
                "amended_count",
            ],
            data_validation_callback=lambda row: len(row) == 34,
            truncate=should_truncate,
        )

        should_truncate = False

    return last_metadata


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_d_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA old format schedule D (debts and obligations) by commiteee data to
    landing table
    """

    should_truncate = True
    last_metadata = None

    for [start_year, end_year] in VA_SCHEDULE_D_OLD_TABLE_TIMELINE:
        last_metadata = va_insert_files_to_landing_of_one_type(
            postgres_pool=pg.pool,
            start_year=start_year,
            end_year=end_year,
            data_file_path="/ScheduleD.csv",
            table_name="scheduled_old_landing",
            table_columns_name=[
                "committee_code",
                "committee_name",
                "report_year",
                "report_due_date",
                "report_code",
                "report_start_date",
                "report_ending_date",
                "fling_source_type",
                "filing_type",
                "amended_count",
                "date_received",
                "filing_status",
                "officer_position",
                "last_name",
                "first_name",
                "middle_name",
                "office_code",
                "office_sub_code",
                "party",
                "entity_name",
                "entity_address",
                "entity_city",
                "entity_state",
                "entity_zip",
                "entity_occupation",
                "entity_place_of_business",
                "trans_type",
                "trans_date",
                "trans_amount",
                "trans_agg_to_date",
                "trans_value_basis",
                "trans_service_or_goods",
                "entity_employer",
            ],
            data_validation_callback=lambda row: len(row) == 33,
            truncate=should_truncate,
        )

        should_truncate = False

    return last_metadata


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_transitional_schedule_d_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert VA transitional format schedule D (debts and obligations) by commiteee
    data to landing table
    """

    should_truncate = True
    last_metadata = None

    for [start_year, end_year] in VA_SCHEDULE_D_TRANSITIONAL_TABLE_TIMELINE:
        last_metadata = va_insert_files_to_landing_of_one_type(
            postgres_pool=pg.pool,
            start_year=start_year,
            end_year=end_year,
            data_file_path="/ScheduleD.csv",
            table_name="scheduled_transitonal_landing",
            table_columns_name=[
                "committee_code",
                "committee_id",
                "committee_type_code",
                "committee_name",
                "report_year",
                "date_received",
                "date_acknowledged",
                "filing_status",
                "filing_type",
                "election_year",
                "officer_position",
                "lastname",
                "firstname",
                "middlename",
                "office_code",
                "office_sub_code",
                "party_desc",
                "sched_id",
                "entity_name",
                "entity_address",
                "entity_city",
                "entity_state",
                "entity_zip",
                "entity_occupation",
                "entity_place_of_business",
                "trans_type",
                "trans_date",
                "trans_amnt",
                "trans_officer",
                "trans_item_or_service",
                "sch_record_key",
                "report_due_date",
                "report_code",
                "report_date",
                "report_ending_date",
                "filing_source_type",
                "amended_count",
            ],
            data_validation_callback=lambda row: len(row) == 37,
            truncate=should_truncate,
        )

        should_truncate = False

    return last_metadata


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_d_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule D (debts and obligations) data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleD.csv",
        table_name="scheduled_new_landing",
        table_columns_name=[
            "scheduledid",
            "reportid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "authorizingname",
            "itemorservice",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 20,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_e_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule E (independent expenditures) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleE_PAC.csv",
        table_name="schedulee_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 30,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_e_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert VA old format schedule E (independent expenditures) by commitee data to
    landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleE.csv",
        table_name="schedulee_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_e_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert VA schedule E (independent expenditures) data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleE.csv",
        table_name="schedulee_new_landing",
        table_columns_name=[
            "reportid",
            "transactiondate",
            "amount",
            "scheduleeid",
            "lendercommitteecontactid",
            "lenderfirstname",
            "lendermiddlename",
            "lenderlastorcompanyname",
            "lenderprefix",
            "lendersuffix",
            "lenderaddressline1",
            "lenderaddressline2",
            "lendercity",
            "lenderstate",
            "lenderzipcode",
            "lenderisindividual",
            "coborrowercommitteecontactid",
            "coborrowerfirstname",
            "coborrowermiddlename",
            "coborrowerlastorcompanyname",
            "coborrowerprefix",
            "coborrowersuffix",
            "coborroweraddressline1",
            "coborroweraddressline2",
            "coborrowercity",
            "coborrowerstate",
            "coborrowerzipcode",
            "coborrowerisindividual",
            "transactiontype",
            "loanbalance",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 32,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_f_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule F (transfers) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleF_PAC.csv",
        table_name="schedulef_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 30,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_f_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert old format VA schedule F (transfers) by commitee data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleF.csv",
        table_name="schedulef_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_f_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule F (transfers) data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleF.csv",
        table_name="schedulef_new_landing",
        table_columns_name=[
            "schedulefid",
            "reportid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "purposeofobligation",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 19,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_g_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """Insert VA schedule G (disbursements) by PACs data to landing table"""

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleG_PAC.csv",
        table_name="scheduleg_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_type",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "election_year",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "sched_id",
            "sum_contrib_a",
            "qty_contrib_a",
            "sum_contrib_b",
            "qty_contrib_b",
            "sub_unitemized_contrib",
            "qty_unitemized_contrib",
            "sum_contrib_c",
            "qty_contrib_c",
            "total_contrib",
            "total_qty_contrib",
            "sum_exp_b",
            "sum_exp_d",
            "begin_loan_bal",
            "sum_loans_recvd_e",
            "sum_loans_paid_e",
            "end_loan_bal",
            "sum_unitemized_contrib2",
            "qty_unitemized_contrib2",
            "sum_unitemized_contrib2_exp",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_g_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert old format VA schedule G (disbursements) by commitee data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleG.csv",
        table_name="scheduleg_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_type",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "election_year",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "sched_id",
            "sum_contrib_a",
            "qty_contrib_a",
            "sum_contrib_b",
            "qty_contrib_b",
            "sub_unitemized_contrib",
            "qty_unitemized_contrib",
            "sum_contrib_c",
            "qty_contrib_c",
            "total_contrib",
            "total_qty_contrib",
            "sum_exp_b",
            "sum_exp_d",
            "begin_loan_bal",
            "sum_loans_recvd_e",
            "sum_loans_paid_e",
            "end_loan_bal",
            "sum_unitemized_contrib2",
            "qty_unitemized_contrib2",
            "sum_unitemized_contrib2_exp",
        ],
        data_validation_callback=lambda row: len(row) == 40,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_g_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule G (disbursements) to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleG.csv",
        table_name="scheduleg_new_landing",
        table_columns_name=[
            "schedulegid",
            "reportid",
            "scheduleacount",
            "scheduleatotal",
            "schedulebcount",
            "schedulebtotal",
            "unitemizedcount",
            "unitemizedtotal",
            "unitemizedinkindcount",
            "unitemizedinkindtotal",
            "allcontributionscount",
            "allcontributionstotal",
            "schedulectotal",
            "schedulebtotalagain",
            "unitemizedinkindtotalagain",
            "scheduledtotal",
            "totalinkindandexpenditures",
            "beginningloanbalance",
            "loansreceivedtotal",
            "loansreceivedandexistingtotal",
            "loanspaidtotal",
            "newloanbalance",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 24,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_h_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule H (disbursements for shared expenses) by PACs data to landing
    table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleH_PAC.csv",
        table_name="scheduleh_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "election_year",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "sched_id",
            "begin_cash_bal",
            "receips_line4_g",
            "receipts_line5_g",
            "receipts_line10_g",
            "tot_expend_funds",
            "exp_line8_g",
            "exp_line12_g",
            "surplus_xferred",
            "tot_surplus_i",
            "end_cash_bal",
            "tot_receipts_to_date",
            "total_exp_to_date",
            "debt_outstanding",
            "bal_start_cycle",
            "tot_receipts_line24_last_rep",
            "tot_receipts_elec_cycle",
            "tot_funds_available",
            "total_disb_last_rep",
            "total_disb_elec_cycle",
            "ending_balance",
        ],
        data_validation_callback=lambda row: len(row) == 38,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_h_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA old format schedule H (disbursements for shared expenses) by commitee data
    to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleH.csv",
        table_name="scheduleh_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "election_year",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "sched_id",
            "begin_cash_bal",
            "receips_line4_g",
            "receipts_line5_g",
            "receipts_line10_g",
            "tot_expend_funds",
            "exp_line8_g",
            "exp_line12_g",
            "surplus_xferred",
            "tot_surplus_i",
            "end_cash_bal",
            "tot_receipts_to_date",
            "total_exp_to_date",
            "debt_outstanding",
            "bal_start_cycle",
            "tot_receipts_line24_last_rep",
            "tot_receipts_elec_cycle",
            "tot_funds_available",
            "total_disb_last_rep",
            "total_disb_elec_cycle",
            "ending_balance",
        ],
        data_validation_callback=lambda row: len(row) == 41,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_h_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule H (disbursements for shared expenses)
    to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleH.csv",
        table_name="scheduleh_new_landing",
        table_columns_name=[
            "schedulehid",
            "reportid",
            "beginningbalance",
            "contributionsreceived",
            "schedulectotal",
            "loansreceivedtotal",
            "contributionsandreceiptsreceived",
            "totalexpendablefunds",
            "totalinkindandexpenditures",
            "loanspaidtotal",
            "scheduleitotal",
            "totalpaymentsmade",
            "expendablefundsbalance",
            "totalunpaiddebts",
            "balanceatstartofelectioncycle",
            "previousreceipts",
            "currentreceipts",
            "totalreceiptsthiselectioncycle",
            "totalfundsavailable",
            "previousdisbursements",
            "currentdisbursements",
            "totaldisbursements",
            "endingbalance",
            "scheduleid",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 25,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_i_pac_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule I (itemized receipts) by PACs data to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleI_PAC.csv",
        table_name="schedulei_pac_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 30,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_old_schedule_i_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA old format schedule I (itemized receipts) by commitee data
    to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        end_year=VA_LAST_YEAR_OF_OLD_FORMAT,
        data_file_path="/ScheduleI.csv",
        table_name="schedulei_old_landing",
        table_columns_name=[
            "committee_code",
            "committee_name",
            "report_year",
            "report_due_date",
            "report_code",
            "report_start_date",
            "report_ending_date",
            "fling_source_type",
            "filing_type",
            "amended_count",
            "date_received",
            "filing_status",
            "officer_position",
            "last_name",
            "first_name",
            "middle_name",
            "office_code",
            "office_sub_code",
            "party",
            "entity_name",
            "entity_address",
            "entity_city",
            "entity_state",
            "entity_zip",
            "entity_occupation",
            "entity_place_of_business",
            "trans_type",
            "trans_date",
            "trans_amount",
            "trans_agg_to_date",
            "trans_value_basis",
            "trans_service_or_goods",
            "entity_employer",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_schedule_i_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA schedule I (itemized receipts) to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/ScheduleI.csv",
        table_name="schedulei_new_landing",
        table_columns_name=[
            "reportid",
            "scheduleid",
            "scheduleiid",
            "committeecontactid",
            "firstname",
            "middlename",
            "lastorcompanyname",
            "prefix",
            "suffix",
            "addressline1",
            "addressline2",
            "city",
            "statecode",
            "zipcode",
            "isindividual",
            "transactiondate",
            "amount",
            "authorizingname",
            "typeofdisposition",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 20,
    )


@dg.asset(deps=[fetch_virginia_campaign_finance], pool="pg")
def va_report_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert VA report (Report.csv) to landing table
    """

    return va_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=VA_FIRST_YEAR_NEW_FORMAT_AVAILABLE,
        data_file_path="/Report.csv",
        table_name="report_landing",
        table_columns_name=[
            "reportid",
            "committeecode",
            "committeename",
            "committeetype",
            "candidatename",
            "isstatewide",
            "isgeneralassembly",
            "islocal",
            "party",
            "fecnumber",
            "reportyear",
            "filingdate",
            "startdate",
            "enddate",
            "addressline1",
            "addressline2",
            "addressline3",
            "city",
            "statecode",
            "zipcode",
            "filingtype",
            "isfinalreport",
            "isamendment",
            "amendmentcount",
            "submitterphone",
            "submitteremail",
            "electioncycle",
            "electioncyclestartdate",
            "electioncycleenddate",
            "officesought",
            "district",
            "noactivity",
            "balancelastreportingperiod",
            "dateofreferendum",
            "submitteddate",
            "accountid",
            "duedate",
            "isxmlupload",
            "reportuid",
        ],
        data_validation_callback=lambda row: len(row) == 39,
    )

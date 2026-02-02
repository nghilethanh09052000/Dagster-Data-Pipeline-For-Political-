import csv
import datetime
import os
import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import fecfile
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.lib.number import is_odd_number
from contributions_pipeline.resources import PostgresResource

FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE = 1980
# We want to focus on the effort of getting the data from 2015 first
FEC_FIRST_DATE_ELECTRONIC_FILED_REPORTS_BULK_DATA_AVAILABLE = datetime.datetime(
    year=2015, month=1, day=1
)


def get_url_for_fec_raw_electronic_filing(date_str: str):
    """
    Get URL for FEC raw filing data (electronic filing) on a specific date.

    Params:
    date_str: standard FEC date string format of `YYYYMMDD`
    """
    return f"https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/{date_str}.zip"


def get_url_for_federal_bulk_download_one_cycle(year: int, prefix: str) -> str:
    """
    Get federal bulk download URL for specific prefix (report type) for a cycle
    """
    return (
        f"https://www.fec.gov/files/bulk-downloads/{year}/{prefix}{str(year)[-2:]}.zip"
    )


def get_result_directory_path_for_federal_bulk_download_one_cycle(
    year: int, prefix: str
) -> str:
    """
    Get the directory path where the un-zipped federal bulk download for cycle year and
    prefix should be
    """
    return f"./fec/{prefix}/{year}"


def get_result_directory_path_for_federal_bulk_download_one_day(
    date_str: str, prefix: str
) -> str:
    """
    Get the directory path where the un-zipped federal bulk download for day and
    prefix should be
    """
    return f"./fec/{prefix}/{date_str}"


def get_file_path_for_combined_federal_one_cycle_bulk_downloads(
    prefix: str,
) -> str:
    """
    Get the file path to the .txt of the combined data for all of the
    FEC one cycle bulk donwloads.
    """
    return f"./fec/{prefix}/combined.txt"


def get_current_cycle_year() -> int:
    """
    Get current FEC cycle year. This will return the even year out of the cycle year.
    For example, cycle year 2025-2026, this will returns 2026.
    """

    current_year = datetime.datetime.now().year
    if is_odd_number(current_year):
        return current_year + 1

    return current_year


def fec_fetch_all_one_cycle_files(
    start_year: int, fec_report_type_prefix: str, human_friendly_name: str | None = None
):
    """
    Most of FEC Bulk Download is download-able based on their cycle. This function will
    download the archive (.zip) file from start year until this year.
    Parameters:
    start_year: the first year (the latest of the cycle year) the data is available,
                e.g. for candidate master is 1979-1980, so put in 1980
    fec_report_type_prefix: the prefix on the bulk download filename for the report
                            type. e.g. for all candidates, the url is
                            fec.gov/files/bulk-downloads/1980/weball80.zip, do the
                            prefix is "weball".
    """

    logger = dg.get_dagster_logger(name=fec_report_type_prefix)
    human_friendly_name = (
        fec_report_type_prefix if human_friendly_name is None else human_friendly_name
    )
    current_cycle_year = get_current_cycle_year()
    first_year = start_year
    last_year_inclusive = current_cycle_year
    step_size = 2

    for year in range(first_year, last_year_inclusive + 1, step_size):
        current_year_cycle_bulk_download_url = (
            get_url_for_federal_bulk_download_one_cycle(
                year=year, prefix=fec_report_type_prefix
            )
        )
        logger.info(
            (
                f"Getting FEC bulk download-able data for {human_friendly_name} on URL",
                current_year_cycle_bulk_download_url,
            ),
        )

        year_extract_path = (
            get_result_directory_path_for_federal_bulk_download_one_cycle(
                year=year, prefix=fec_report_type_prefix
            )
        )
        Path(year_extract_path).mkdir(parents=True, exist_ok=True)

        temp_file_path = os.path.join(year_extract_path, f"temp_archive_{year}.zip")

        stream_download_file_to_path(
            request_url=current_year_cycle_bulk_download_url,
            file_save_path=temp_file_path,
        )

        try:
            with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                for file_info in zip_ref.infolist():
                    target_path = os.path.join(year_extract_path, file_info.filename)

                    if file_info.filename.endswith("/"):  # It's a directory
                        os.makedirs(target_path, exist_ok=True)
                        continue

                    os.makedirs(os.path.dirname(target_path), exist_ok=True)

                    # Extract file using buffered streaming
                    with (
                        zip_ref.open(file_info) as source,
                        open(target_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)  # 64KB buffer
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)


def fec_insert_files_to_landing_of_one_type(
    postgres_pool: ConnectionPool,
    start_year: int,
    fec_report_type_prefix: str,
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
    fec_report_type_prefix: the prefix on the bulk download filename for the report
                            type. e.g. for all candidates, the url is
                            fec.gov/files/bulk-downloads/1980/weball80.zip, do the
                            prefix is "weball".
    data_file_path: the file path of the actual data file on the year folder, for
                    example individual contributions, the local folder for it is laid
                    out as it follows (year) -> itcont.txt. The data file is itcont.txt,
                    thus the input should be "/itcont.txt".
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for individual contributions.
                        ["cmte_id", "amndt_ind", "rpt_tp,transaction_pgi", "image_num",
                         "transaction_tp", ..., "sub_id"]
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    """

    logger = dg.get_dagster_logger(fec_report_type_prefix)

    truncate_query = get_sql_truncate_query(table_name=table_name)

    current_cycle_year = get_current_cycle_year()

    first_year = start_year
    last_year_inclusive = current_cycle_year
    step_size = 2

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        for year in range(first_year, last_year_inclusive + 1, step_size):
            base_path = get_result_directory_path_for_federal_bulk_download_one_cycle(
                year=year, prefix=fec_report_type_prefix
            )
            current_cycle_data_file_path = base_path + data_file_path

            logger.info(
                f"Inserting year {year} file to pg ({current_cycle_data_file_path})"
            )

            try:
                current_cycle_file_lines_generator = safe_readline_csv_like_file(
                    file_path=current_cycle_data_file_path
                )
                parsed_current_cycle_file = csv.reader(
                    current_cycle_file_lines_generator,
                    delimiter="|",
                    quotechar=None,
                )

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


fec_schedule_a_daily_partitions = dg.DailyPartitionsDefinition(
    start_date=FEC_FIRST_DATE_ELECTRONIC_FILED_REPORTS_BULK_DATA_AVAILABLE,
    timezone="America/New_York",
    fmt="%Y%m%d",
)


@dg.asset(partitions_def=fec_schedule_a_daily_partitions)
def fec_daily_schedule_a(context: dg.AssetExecutionContext) -> None:
    """
    Daily partition of FEC raw filing data on individual contributions.

    The asset will download the files from each year and parse the itemized
    'SA' (schedule A) data, parse it, and convert it to
    `federal_individual_contributions_landing` table standard. Only then it will
    insert the converted file to `fec/indiv/{date}/itcont.txt`.
    """

    raw_one_day_filing_base_path = Path(
        get_result_directory_path_for_federal_bulk_download_one_day(
            date_str=context.partition_key, prefix="sa"
        )
    )
    raw_one_day_filing_temp_path = raw_one_day_filing_base_path / "temp"
    raw_one_day_filing_temp_path.mkdir(parents=True, exist_ok=True)

    temp_zip_file_path = raw_one_day_filing_temp_path / "temp.zip"
    extract_path = raw_one_day_filing_temp_path

    parsed_result_file_path = raw_one_day_filing_base_path / "schedule_a.txt"

    try:
        stream_download_file_to_path(
            request_url=get_url_for_fec_raw_electronic_filing(
                date_str=context.partition_key
            ),
            file_save_path=temp_zip_file_path,
        )

        with zipfile.ZipFile(temp_zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                target_path = extract_path / file_info.filename

                if file_info.filename.endswith("/"):  # It's a directory
                    target_path.mkdir(parents=True, exist_ok=True)
                    continue

                target_path.parent.mkdir(parents=True, exist_ok=True)

                # Extract file using buffered streaming
                with (
                    zip_ref.open(file_info) as source,
                    open(target_path, "wb") as target,
                ):
                    shutil.copyfileobj(source, target, 65536)  # 64KB buffer

        with open(parsed_result_file_path, "w") as output_file:
            output_writer = csv.writer(
                output_file, delimiter="|", quotechar=None, escapechar="\\"
            )

            for file in extract_path.glob("*.fec"):
                ignored_filings_counter = 0
                filings_counter = 0

                for item in fecfile.iter_file(
                    # Parse only schedule A filings itemization
                    file_path=file,
                    options={
                        # This filters works by check if the FEC Line Number starts
                        # with the string given. So "SA11" will match "SA11B", "SA11B",
                        # etc.
                        #
                        # Hard to find source from FEC, found a good cheat sheet from
                        # GOP campaign CRM:
                        # https://support.cmdi.com/hc/en-us/article_attachments/11599289679892
                        "filter_itemizations": [
                            # Get all Schedule A data
                            "SA"
                        ],
                        # Ignore data type parsing, keep it as string instead
                        "as_strings": True,
                    },
                ):
                    if item.data_type != "itemization":
                        context.log.info("Ignoring text or summary type")

                        ignored_filings_counter += 1
                        continue

                    # WARN:
                    # item.data is a dict as the docs references for from_file, etc.
                    # Even though we're using the iter_file, the attributes of FecFile
                    # named "data" is a dict. Thus only .get(name, default) will works,
                    # getattr will not!

                    # NOTE: references to different data type name:
                    # https://github.com/esonderegger/fecfile/blob/main/fecfile/mappings.json

                    form_type = item.data.get("form_type")

                    if form_type is None:
                        context.log.warning(
                            f"No form type on file {file},"
                            f" on date {context.partition_key}",
                            item.data,
                        )

                        ignored_filings_counter += 1
                        continue

                    # Bundled registrants or lobbyist contributions, have significantly
                    # different schema. Ref:
                    # https://github.com/esonderegger/fecfile/blob/0ac4f0372d4eddedd6bde850cd73c092a5ffc76b/fecfile/mappings.json#L9160
                    if str(form_type).lower().startswith("sa3l"):
                        context.log.info(
                            "Ignoring bundled contributions by lobbyist/registrants"
                        )

                        ignored_filings_counter += 1
                        continue

                    transformed_row = [
                        # partition_date
                        context.partition_key,
                        # form_type
                        form_type,
                        # filer_committee_id_number
                        item.data.get("filer_committee_id_number", None),
                        # contributor_name
                        item.data.get("contributor_name", None),
                        # contributor_street_1
                        item.data.get("contributor_street_1", None),
                        # contributor_street_2
                        item.data.get("contributor_street_2", None),
                        # contributor_city
                        item.data.get("contributor_city", None),
                        # contributor_state
                        item.data.get("contributor_state", None),
                        # contributor_zip_code
                        item.data.get("contributor_zip_code", None),
                        # election_code
                        item.data.get("election_code", None),
                        # election_other_description
                        item.data.get("election_other_description", None),
                        # contributor_employer
                        item.data.get("contributor_employer", None),
                        # contributor_occupation
                        item.data.get("contributor_occupation", None),
                        # contribution_aggregate
                        item.data.get("contribution_aggregate", None),
                        # contribution_date
                        item.data.get("contribution_date", None),
                        # contribution_amount
                        item.data.get("contribution_amount", None),
                        # contribution_purpose_code
                        item.data.get("contribution_purpose_code", None),
                        # contribution_purpose_descrip
                        item.data.get("contribution_purpose_descrip", None),
                        # conduit_name
                        item.data.get("conduit_name", None),
                        # conduit_street1
                        item.data.get("conduit_street1", None),
                        # conduit_street2
                        item.data.get("conduit_street2", None),
                        # conduit_city
                        item.data.get("conduit_city", None),
                        # conduit_state
                        item.data.get("conduit_state", None),
                        # conduit_zip_code
                        item.data.get("conduit_zip_code", None),
                        # donor_committee_fec_id
                        item.data.get("donor_committee_fec_id", None),
                        # donor_candidate_fec_id
                        item.data.get("donor_candidate_fec_id", None),
                        # donor_candidate_name
                        item.data.get("donor_candidate_name", None),
                        # donor_candidate_office
                        item.data.get("donor_candidate_office", None),
                        # donor_candidate_state
                        item.data.get("donor_candidate_state", None),
                        # donor_candidate_district
                        item.data.get("donor_candidate_district", None),
                        # memo_code
                        item.data.get("memo_code", None),
                        # memo_text_description
                        item.data.get("memo_text_description", None),
                        # amended_cd
                        item.data.get("amended_cd", None),
                        # entity_type
                        item.data.get("entity_type", None),
                        # transaction_id
                        item.data.get("transaction_id", None),
                        # back_reference_tran_id_number
                        item.data.get("back_reference_tran_id_number", None),
                        # back_reference_sched_name
                        item.data.get("back_reference_sched_name", None),
                        # reference_code
                        item.data.get("reference_code", None),
                        # increased_limit_code
                        item.data.get("increased_limit_code", None),
                        # contributor_organization_name
                        item.data.get("contributor_organization_name", None),
                        # contributor_last_name
                        item.data.get("contributor_last_name", None),
                        # contributor_first_name
                        item.data.get("contributor_first_name", None),
                        # contributor_middle_name
                        item.data.get("contributor_middle_name", None),
                        # contributor_prefix
                        item.data.get("contributor_prefix", None),
                        # contributor_suffix
                        item.data.get("contributor_suffix", None),
                        # donor_candidate_last_name
                        item.data.get("donor_candidate_last_name", None),
                        # donor_candidate_first_name
                        item.data.get("donor_candidate_first_name", None),
                        # donor_candidate_middle_name
                        item.data.get("donor_candidate_middle_name", None),
                        # donor_candidate_prefix
                        item.data.get("donor_candidate_prefix", None),
                        # donor_candidate_suffix
                        item.data.get("donor_candidate_suffix", None),
                        # donor_committee_name
                        item.data.get("donor_committee_name", None),
                        # image_number
                        item.data.get("image_number", None),
                    ]

                    output_writer.writerow(transformed_row)

                    filings_counter += 1

                context.log.info(
                    f"On {file} ({context.partition_key}), got {filings_counter} valid"
                    f" filings, ({ignored_filings_counter} ignored rows)"
                )

    except RuntimeError as e:
        context.log.error(f"Error while getting data for {context.partition_key}, {e}")


@dg.asset(
    partitions_def=fec_schedule_a_daily_partitions,
    deps=[fec_daily_schedule_a],
    pool="pg",
)
def fec_daily_schedule_a_insert_to_landing_table(
    context: dg.AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
) -> None:
    """
    Insert the day processed raw FEC individual contributions to landing table
    """

    with (
        pg.pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        # context.log.info(
        #     "Deleting rows from this partition before inserting the new one"
        # )
        # Only delete files from this day only
        # TODO: re-anble this if we want to make sure there's no duplicate keys
        # pg_cursor.execute(
        #     query=sql.SQL(
        #         "DELETE FROM federal_schedule_a_landing "
        #         "WHERE partition_date = %s"
        #     ),
        #     params=(context.partition_key,),
        # )

        raw_one_day_filing_base_path = Path(
            get_result_directory_path_for_federal_bulk_download_one_day(
                date_str=context.partition_key, prefix="sa"
            )
        )
        current_day_data_file_path = raw_one_day_filing_base_path / "schedule_a.txt"

        context.log.info(
            f"Inserting date {context.partition_key} file to"
            f" pg ({current_day_data_file_path})"
        )

        try:
            current_cycle_file_lines_generator = safe_readline_csv_like_file(
                file_path=current_day_data_file_path
            )
            parsed_current_cycle_file = csv.reader(
                current_cycle_file_lines_generator,
                delimiter="|",
                quotechar=None,
                escapechar="\\",
            )

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_current_cycle_file,
                table_name="federal_schedule_a_landing",
                table_columns_name=[
                    "partition_date",
                    "form_type",
                    "filer_committee_id_number",
                    "contributor_name",
                    "contributor_street_1",
                    "contributor_street_2",
                    "contributor_city",
                    "contributor_state",
                    "contributor_zip_code",
                    "election_code",
                    "election_other_description",
                    "contributor_employer",
                    "contributor_occupation",
                    "contribution_aggregate",
                    "contribution_date",
                    "contribution_amount",
                    "contribution_purpose_code",
                    "contribution_purpose_descrip",
                    "conduit_name",
                    "conduit_street1",
                    "conduit_street2",
                    "conduit_city",
                    "conduit_state",
                    "conduit_zip_code",
                    "donor_committee_fec_id",
                    "donor_candidate_fec_id",
                    "donor_candidate_name",
                    "donor_candidate_office",
                    "donor_candidate_state",
                    "donor_candidate_district",
                    "memo_code",
                    "memo_text_description",
                    "amended_cd",
                    "entity_type",
                    "transaction_id",
                    "back_reference_tran_id_number",
                    "back_reference_sched_name",
                    "reference_code",
                    "increased_limit_code",
                    "contributor_organization_name",
                    "contributor_last_name",
                    "contributor_first_name",
                    "contributor_middle_name",
                    "contributor_prefix",
                    "contributor_suffix",
                    "donor_candidate_last_name",
                    "donor_candidate_first_name",
                    "donor_candidate_middle_name",
                    "donor_candidate_prefix",
                    "donor_candidate_suffix",
                    "donor_committee_name",
                    "image_number",
                ],
                row_validation_callback=lambda row: len(row) == 52,
            )

        except FileNotFoundError:
            context.log.warning(
                f"File for date {context.partition_key} is non-existent, ignoring..."
            )
        except Exception as e:
            context.log.error(
                f"Got error while reading date {context.partition_key} file: {e}"
            )
            raise e


@dg.asset()
def fec_committee_master():
    """
    Record for each commitee registered with FEC. Includes PAC, party committee,
    compaign commitee for presidential, house, and senate. Also groups or
    organizations who spends moeny for or against candidates for federal office.

    Data starts from 1980, the asset will download all until the current
    cycle year.
    """

    fec_fetch_all_one_cycle_files(
        start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        fec_report_type_prefix="cm",
        human_friendly_name="commitee master",
    )


@dg.asset(deps=[fec_committee_master], pool="pg")
def fec_committee_master_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all the ingested data to landing table
    "federal_committee_master_landing".

    As it is master data, if in the later year there's the same commitee
    with the same id. It will update the commitee with the latest data.
    It doesn't store old historic data, just the latest one.

    This asset assumed that all of the files have the same columns.
    """

    return fec_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        fec_report_type_prefix="cm",
        data_file_path="/cm.txt",
        table_name="federal_committee_master_landing",
        table_columns_name=[
            "cmte_id",
            "cmte_nm",
            "tres_nm",
            "cmte_st1",
            "cmte_st2",
            "cmte_city",
            "cmte_st",
            "cmte_zip",
            "cmte_dsgn",
            "cmte_tp",
            "cmte_pty_affiliation",
            "cmte_filing_freq",
            "org_tp",
            "connected_org_nm",
            "cand_id",
        ],
        data_validation_callback=lambda row: len(row) == 15,
    )


@dg.asset()
def fec_candidate_master():
    """
    Record of all candidate who has registered with FEC or appear on a ballot list
    prepared by state election office.

    Data starts from 1980, the asset will donwload all until the current cycle year
    """

    fec_fetch_all_one_cycle_files(
        start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        fec_report_type_prefix="cn",
        human_friendly_name="candidate master",
    )


@dg.asset(deps=[fec_candidate_master], pool="pg")
def fec_candidate_master_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all the ingested data to landing table "federal_candidate_master_landing"

    Each candidate will have the same cand_id if the candidate is running for the same
    office. Thus if we found the same cand_id, we could assume that the data is the
    latest data on their campaign for the same office.

    This asset assume that all the files have the same columns.
    """

    return fec_insert_files_to_landing_of_one_type(
        postgres_pool=pg.pool,
        start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        fec_report_type_prefix="cn",
        data_file_path="/cn.txt",
        table_name="federal_candidate_master_landing",
        table_columns_name=[
            "cand_id",
            "cand_name",
            "cand_pty_affiliation",
            "cand_election_yr",
            "cand_office_st",
            "cand_office",
            "cand_office_district",
            "cand_ici",
            "cand_status",
            "cand_pcc",
            "cand_st1",
            "cand_st2",
            "cand_city",
            "cand_st",
            "cand_zip",
        ],
        data_validation_callback=lambda row: len(row) == 15,
    )

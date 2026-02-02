import csv
import io
import shutil
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
from psycopg_pool import ConnectionPool

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import insert_parsed_file_to_landing_table
from contributions_pipeline.resources import PostgresResource

NY_DATA_BASE_PATH = "./states/new_york"

NY_SUPABASE_STORAGE_BASE_URL = "https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/ny/"


def ny_download_extract_all_time_data(source_url: str, output_file_path: Path):
    """
    Download zips created by NY public reporting site (from our Supabase Storage) and
    extract the csv content of the zip.

    This function is smart enough to be used for both Filer Data and All Time Disclosure
    Report on all report type.

    All Time Disclosure Report all report type use zips inside of zips, this function
    will keep unzip it. More than the second depth and no CSV will result
    in RuntimeError.

    While Filder Data just have one depth of zip. If the function see csv file it will
    extract that and return.

    Params:
    source_url: Supabase Storage public URL to download the data
    output_file_path: the resulting csv data path
    """
    logger = dg.get_dagster_logger("ny_download_extract")

    base_path = Path(NY_DATA_BASE_PATH)
    temp_zip_dir_path = base_path / "temp"
    temp_zip_dir_path.mkdir(parents=True, exist_ok=True)

    temp_zip_file_path = temp_zip_dir_path / f"{output_file_path.name}.zip"

    try:
        logger.info(f"Downloading data from {source_url}")
        stream_download_file_to_path(
            request_url=source_url, file_save_path=temp_zip_file_path
        )

        depth = 0

        zip_ref = ZipFile(file=temp_zip_file_path, mode="r")

        while True:
            if depth >= 2:
                raise RuntimeError("Nested Zip is more than 2 depth!")

            found_csv = False
            found_zip = False

            for file_info in zip_ref.infolist():
                logger.info(f"Checking file {file_info.filename}...")

                lowered_filename = file_info.filename.lower()

                if lowered_filename.endswith(".csv"):
                    logger.info(
                        f"Found csv on depth {depth} ({file_info.filename}), "
                        f"extracting the data to {output_file_path}"
                    )

                    with (
                        zip_ref.open(file_info) as source,
                        open(output_file_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)  # 64KB buffer

                    found_csv = True
                    break
                elif lowered_filename.endswith(".zip"):
                    logger.info(
                        f"Found zip on depth {depth} ({file_info.filename}), "
                        "going deeper!"
                    )
                    with zip_ref.open(file_info) as nested_zip_source:
                        # NOTE: change this implementation if OOM occurs. The csv files
                        # currently are 4GB for state commitee, and 2.3GB for county
                        # commitee (this will get bigger as time goes as this is all
                        # time data). So if OOM, extract to file first, then
                        # shutil.copyfileobj the csv file to a file target.
                        zip_file_data = io.BytesIO(nested_zip_source.read())

                        # Close previous zip_ref
                        zip_ref.close()
                        # Open the nested ones now
                        zip_ref = ZipFile(file=zip_file_data)

                    found_zip = True
                    depth += 1
                    break

            if not found_csv and not found_zip:
                raise RuntimeError(
                    f"On nested zip depth {depth + 1} there's no csv and no zip!"
                )
            elif found_csv:
                break

    finally:
        temp_zip_file_path.unlink(missing_ok=True)


def ny_insert_data_to_landing_table(
    pg_pool: ConnectionPool,
    source_file_path: Path,
    table_name: str,
    table_columns: list[str],
):
    """
    Insert raw data from public reporting NY to a landing table.

    Params:
    pg_pool: postgres connection pool, this can be retrieved from pg resource
    source_file_path: the file path to the raw file
    table_name: the landing table name the data is going to be inserted into
    table_columns: list of columns sorted the same as the raw file as well
    """
    logger = dg.get_dagster_logger(f"ny_insert_{table_name}")

    logger.info(f"Inserting file {source_file_path} file to table {table_name}")

    current_cycle_file_lines_generator = safe_readline_csv_like_file(
        file_path=source_file_path,
    )
    parsed_current_cycle_file = csv.reader(
        current_cycle_file_lines_generator,
        delimiter=",",
        quotechar='"',
    )

    with pg_pool.connection() as pg_connection, pg_connection.cursor() as pg_cursor:
        insert_parsed_file_to_landing_table(
            pg_cursor=pg_cursor,
            csv_reader=parsed_current_cycle_file,
            table_name=table_name,
            table_columns_name=table_columns,
            row_validation_callback=lambda row: len(row) == len(table_columns),
        )


@dg.asset()
def ny_all_time_state_candidate():
    """
    This asset will download the statically hosted all time state candidate file
    in Supabase Storage and extract the csv out of it.
    """

    output_file_path = Path(NY_DATA_BASE_PATH) / "all_time_state_candidate.csv"

    ny_download_extract_all_time_data(
        source_url=NY_SUPABASE_STORAGE_BASE_URL + "ALL_REPORTS_StateCandidate.zip",
        output_file_path=output_file_path,
    )


@dg.asset(deps=[ny_all_time_state_candidate])
def ny_insert_all_time_state_candidate_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all of the state candidate raw data to landing table
    """

    raw_file_path = Path(NY_DATA_BASE_PATH) / "all_time_state_candidate.csv"

    ny_insert_data_to_landing_table(
        pg_pool=pg.pool,
        source_file_path=raw_file_path,
        table_name="ny_state_candidate_landing",
        table_columns=[
            "FILER_ID",
            "FILER_PREVIOUS_ID",
            "CAND_COMM_NAME",
            "ELECTION_YEAR",
            "ELECTION_TYPE",
            "COUNTY_DESC",
            "FILING_ABBREV",
            "FILING_DESC",
            "R_AMEND",
            "FILING_CAT_DESC",
            "FILING_SCHED_ABBREV",
            "FILING_SCHED_DESC",
            "LOAN_LIB_NUMBER",
            "TRANS_NUMBER",
            "TRANS_MAPPING",
            "SCHED_DATE",
            "ORG_DATE",
            "CNTRBR_TYPE_DESC",
            "CNTRBN_TYPE_DESC",
            "TRANSFER_TYPE_DESC",
            "RECEIPT_TYPE_DESC",
            "RECEIPT_CODE_DESC",
            "PURPOSE_CODE_DESC",
            "R_SUBCONTRACTOR",
            "FLNG_ENT_NAME",
            "FLNG_ENT_FIRST_NAME",
            "FLNG_ENT_MIDDLE_NAME",
            "FLNG_ENT_LAST_NAME",
            "FLNG_ENT_ADD1",
            "FLNG_ENT_CITY",
            "FLNG_ENT_STATE",
            "FLNG_ENT_ZIP",
            "FLNG_ENT_COUNTRY",
            "PAYMENT_TYPE_DESC",
            "PAY_NUMBER",
            "OWED_AMT",
            "ORG_AMT",
            "LOAN_OTHER_DESC",
            "TRANS_EXPLNTN",
            "R_ITEMIZED",
            "R_LIABILITY",
            "ELECTION_YEAR_R",
            "OFFICE_DESC",
            "DISTRICT",
            "DIST_OFF_CAND_BAL_PROP",
        ],
    )


@dg.asset()
def ny_all_time_county_candidate():
    """
    This asset will download the statically hosted all time county candidate file
    in Supabase Storage and extract the csv out of it.
    """

    output_file_path = Path(NY_DATA_BASE_PATH) / "all_time_county_candidate.csv"

    ny_download_extract_all_time_data(
        source_url=NY_SUPABASE_STORAGE_BASE_URL + "ALL_REPORTS_CountyCandidate.zip",
        output_file_path=output_file_path,
    )


@dg.asset(deps=[ny_all_time_county_candidate])
def ny_insert_all_time_county_candidate_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all of the county candidate raw data to landing table
    """

    raw_file_path = Path(NY_DATA_BASE_PATH) / "all_time_county_candidate.csv"

    ny_insert_data_to_landing_table(
        pg_pool=pg.pool,
        source_file_path=raw_file_path,
        table_name="ny_county_candidate_landing",
        table_columns=[
            "FILER_ID",
            "FILER_PREVIOUS_ID",
            "CAND_COMM_NAME",
            "ELECTION_YEAR",
            "ELECTION_TYPE",
            "COUNTY_DESC",
            "FILING_ABBREV",
            "FILING_DESC",
            "R_AMEND",
            "FILING_CAT_DESC",
            "FILING_SCHED_ABBREV",
            "FILING_SCHED_DESC",
            "LOAN_LIB_NUMBER",
            "TRANS_NUMBER",
            "TRANS_MAPPING",
            "SCHED_DATE",
            "ORG_DATE",
            "CNTRBR_TYPE_DESC",
            "CNTRBN_TYPE_DESC",
            "TRANSFER_TYPE_DESC",
            "RECEIPT_TYPE_DESC",
            "RECEIPT_CODE_DESC",
            "PURPOSE_CODE_DESC",
            "R_SUBCONTRACTOR",
            "FLNG_ENT_NAME",
            "FLNG_ENT_FIRST_NAME",
            "FLNG_ENT_MIDDLE_NAME",
            "FLNG_ENT_LAST_NAME",
            "FLNG_ENT_ADD1",
            "FLNG_ENT_CITY",
            "FLNG_ENT_STATE",
            "FLNG_ENT_ZIP",
            "FLNG_ENT_COUNTRY",
            "PAYMENT_TYPE_DESC",
            "PAY_NUMBER",
            "OWED_AMT",
            "ORG_AMT",
            "LOAN_OTHER_DESC",
            "TRANS_EXPLNTN",
            "R_ITEMIZED",
            "R_LIABILITY",
            "ELECTION_YEAR_R",
            "OFFICE_DESC",
            "DISTRICT",
            "DIST_OFF_CAND_BAL_PROP",
        ],
    )


@dg.asset()
def ny_all_time_state_committee():
    """
    This asset will download the statically hosted all time state committee file
    in Supabase Storage and extract the csv out of it.
    """

    output_file_path = Path(NY_DATA_BASE_PATH) / "all_time_state_committee.csv"

    ny_download_extract_all_time_data(
        source_url=NY_SUPABASE_STORAGE_BASE_URL + "ALL_REPORTS_StateCommittee.zip",
        output_file_path=output_file_path,
    )


@dg.asset(deps=[ny_all_time_state_committee])
def ny_insert_all_time_state_committee_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all of the state committee raw data to landing table
    """

    raw_file_path = Path(NY_DATA_BASE_PATH) / "all_time_state_committee.csv"

    ny_insert_data_to_landing_table(
        pg_pool=pg.pool,
        source_file_path=raw_file_path,
        table_name="ny_state_committee_landing",
        table_columns=[
            "FILER_ID",
            "FILER_PREVIOUS_ID",
            "CAND_COMM_NAME",
            "ELECTION_YEAR",
            "ELECTION_TYPE",
            "COUNTY_DESC",
            "FILING_ABBREV",
            "FILING_DESC",
            "R_AMEND",
            "FILING_CAT_DESC",
            "FILING_SCHED_ABBREV",
            "FILING_SCHED_DESC",
            "LOAN_LIB_NUMBER",
            "TRANS_NUMBER",
            "TRANS_MAPPING",
            "SCHED_DATE",
            "ORG_DATE",
            "CNTRBR_TYPE_DESC",
            "CNTRBN_TYPE_DESC",
            "TRANSFER_TYPE_DESC",
            "RECEIPT_TYPE_DESC",
            "RECEIPT_CODE_DESC",
            "PURPOSE_CODE_DESC",
            "R_SUBCONTRACTOR",
            "FLNG_ENT_NAME",
            "FLNG_ENT_FIRST_NAME",
            "FLNG_ENT_MIDDLE_NAME",
            "FLNG_ENT_LAST_NAME",
            "FLNG_ENT_ADD1",
            "FLNG_ENT_CITY",
            "FLNG_ENT_STATE",
            "FLNG_ENT_ZIP",
            "FLNG_ENT_COUNTRY",
            "PAYMENT_TYPE_DESC",
            "PAY_NUMBER",
            "OWED_AMT",
            "ORG_AMT",
            "LOAN_OTHER_DESC",
            "TRANS_EXPLNTN",
            "R_ITEMIZED",
            "R_LIABILITY",
            "ELECTION_YEAR_R",
            "OFFICE_DESC",
            "DISTRICT",
            "DIST_OFF_CAND_BAL_PROP",
        ],
    )


@dg.asset()
def ny_all_time_county_committee():
    """
    This asset will download the statically hosted all time county committee file
    in Supabase Storage and extract the csv out of it.
    """

    output_file_path = Path(NY_DATA_BASE_PATH) / "all_time_county_committee.csv"

    ny_download_extract_all_time_data(
        source_url=NY_SUPABASE_STORAGE_BASE_URL + "ALL_REPORTS_CountyCommittee.zip",
        output_file_path=output_file_path,
    )


@dg.asset(deps=[ny_all_time_county_committee])
def ny_insert_all_time_county_committee_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Insert all of the county committee raw data to landing table
    """

    raw_file_path = Path(NY_DATA_BASE_PATH) / "all_time_county_committee.csv"

    ny_insert_data_to_landing_table(
        pg_pool=pg.pool,
        source_file_path=raw_file_path,
        table_name="ny_county_committee_landing",
        table_columns=[
            "FILER_ID",
            "FILER_PREVIOUS_ID",
            "CAND_COMM_NAME",
            "ELECTION_YEAR",
            "ELECTION_TYPE",
            "COUNTY_DESC",
            "FILING_ABBREV",
            "FILING_DESC",
            "R_AMEND",
            "FILING_CAT_DESC",
            "FILING_SCHED_ABBREV",
            "FILING_SCHED_DESC",
            "LOAN_LIB_NUMBER",
            "TRANS_NUMBER",
            "TRANS_MAPPING",
            "SCHED_DATE",
            "ORG_DATE",
            "CNTRBR_TYPE_DESC",
            "CNTRBN_TYPE_DESC",
            "TRANSFER_TYPE_DESC",
            "RECEIPT_TYPE_DESC",
            "RECEIPT_CODE_DESC",
            "PURPOSE_CODE_DESC",
            "R_SUBCONTRACTOR",
            "FLNG_ENT_NAME",
            "FLNG_ENT_FIRST_NAME",
            "FLNG_ENT_MIDDLE_NAME",
            "FLNG_ENT_LAST_NAME",
            "FLNG_ENT_ADD1",
            "FLNG_ENT_CITY",
            "FLNG_ENT_STATE",
            "FLNG_ENT_ZIP",
            "FLNG_ENT_COUNTRY",
            "PAYMENT_TYPE_DESC",
            "PAY_NUMBER",
            "OWED_AMT",
            "ORG_AMT",
            "LOAN_OTHER_DESC",
            "TRANS_EXPLNTN",
            "R_ITEMIZED",
            "R_LIABILITY",
            "ELECTION_YEAR_R",
            "OFFICE_DESC",
            "DISTRICT",
            "DIST_OFF_CAND_BAL_PROP",
        ],
    )


@dg.asset()
def ny_filer_data():
    """
    This asset will download the statically hosted filer data file
    in Supabase Storage and extract the csv out of it.
    """

    output_file_path = Path(NY_DATA_BASE_PATH) / "filer_data.csv"

    ny_download_extract_all_time_data(
        source_url=NY_SUPABASE_STORAGE_BASE_URL + "commcand.zip",
        output_file_path=output_file_path,
    )


@dg.asset(deps=[ny_filer_data])
def ny_insert_filer_data_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Insert all of the filer raw data to landing table
    """

    raw_file_path = Path(NY_DATA_BASE_PATH) / "filer_data.csv"

    ny_insert_data_to_landing_table(
        pg_pool=pg.pool,
        source_file_path=raw_file_path,
        table_name="ny_filer_data_landing",
        table_columns=[
            "FILER_ID",
            "FILER_NAME",
            "COMPLIANCE_TYPE_DESC",
            "FILER_TYPE_DESC",
            "FILER_STATUS",
            "COMMITTEE_TYPE_DESC",
            "OFFICE_DESC",
            "DISTRICT",
            "COUNTY_DESC",
            "MUNICIPALITY_DESC_OR_SUBDIVISION_DESC",
            "TREASURER_FIRST_NAME",
            "TREASURER_MIDDLE_NAME",
            "TREASURER_LAST_NAME",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIPCODE",
        ],
    )

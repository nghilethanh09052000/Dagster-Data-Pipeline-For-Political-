import csv
from datetime import datetime
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext

from contributions_pipeline.lib.fetch import stream_download_file_to_path
from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_truncate_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource

# Configuration
OH_DATA_BASE_PATH = "./states/ohio"
OH_SUPABASE_STORAGE_URL = "https://nhzyllufscrztbhzbpvt.supabase.co/storage/v1/object/public/hardcoded-downloads/oh/"
START_YEAR = 1990
LAST_YEAR = datetime.now().year
YEARS = range(START_YEAR, LAST_YEAR + 1)

# File patterns and static files (should be organized together)
FILE_CONFIG = {
    "patterns": {
        "candidate_files": [
            "ALL_CAN_CON_",
            "ALL_CAN_EXP_",
            "CAC_CON_",
            "CAC_EXP_",
            "CAN_CON_",
            "CAN_EXP_",
        ],
        "pac_files": ["ALL_PAC_CON_", "ALL_PAC_EXP_", "PAC_CON_", "PAC_EXP_"],
        "party_files": [
            "ALL_PAR_CON_",
            "ALL_PAR_EXP_",
            "PPC_CON_",
            "PPC_EXP_",
            "PAR_CON_",
            "PAR_EXP_",
        ],
        "new_files": [
            "ALL_CAN_CON_",
            "ALL_CAN_EXP_",
            "CAC_CON_",
            "CAC_EXP_",
            "CAN_CON_",
            "CAN_EXP_",
            "ALL_PAC_CON_",
            "ALL_PAC_EXP_",
            "PAC_CON_",
            "PAC_EXP_",
            "ALL_PAR_CON_",
            "ALL_PAR_EXP_",
            "PPC_CON_",
            "PPC_EXP_",
            "PAR_CON_",
            "PAR_EXP_",
        ],
    },
    "static_files": {
        "candidate_files": [
            "ACT_CAN_LIST.CSV",
            "CAC_CON_MSTRKEY_10592.CSV",
            "CAC_CON_MSTRKEY_12822.CSV",
            "CAC_CON_MSTRKEY_12883.CSV",
            "CAC_CON_MSTRKEY_13504.CSV",
            "CAC_CON_MSTRKEY_17.CSV",
            "CAC_CON_MSTRKEY_289.CSV",
            "CAC_CON_MSTRKEY_6662.CSV",
            "CAC_CON_MSTRKEY_7400.CSV",
            "CAC_CON_MSTRKEY_9497.CSV",
            "CAC_EXP_MSTRKEY_10592.CSV",
            "CAC_EXP_MSTRKEY_12822.CSV",
            "CAC_EXP_MSTRKEY_12883.CSV",
            "CAC_EXP_MSTRKEY_13504.CSV",
            "CAC_EXP_MSTRKEY_17.CSV",
            "CAC_EXP_MSTRKEY_289.CSV",
            "CAC_EXP_MSTRKEY_6662.CSV",
            "CAC_EXP_MSTRKEY_7400.CSV",
            "CAC_EXP_MSTRKEY_9497.CSV",
            "CAN_COVER.CSV",
        ],
        "new_files": [
            "CAC_CON_MSTRKEY_10592_2024.CSV",
            "CAC_CON_MSTRKEY_11239.CSV",
            "CAC_CON_MSTRKEY_12822_2024.CSV",
            "CAC_CON_MSTRKEY_12883_2024.CSV",
            "CAC_CON_MSTRKEY_13152_2024.CSV",
            "CAC_CON_MSTRKEY_13504_2024.CSV",
            "CAC_CON_MSTRKEY_14779_2024.CSV",
            "CAC_CON_MSTRKEY_14814_2024.CSV",
            "CAC_CON_MSTRKEY_14817_2024.CSV",
            "CAC_CON_MSTRKEY_15242_2024.CSV",
            "CAC_CON_MSTRKEY_15259_2024.CSV",
            "CAC_CON_MSTRKEY_15838_2024.CSV",
            "CAC_CON_MSTRKEY_15904_2024.CSV",
            "CAC_CON_MSTRKEY_583_2024.CSV",
            "CAC_CON_MSTRKEY_6541_2024.CSV",
            "CAC_CON_MSTRKEY_7400_2024.CSV",
            "CAC_EXP_MSTRKEY_10592_2024.CSV",
            "CAC_EXP_MSTRKEY_10659_2024.CSV",
            "CAC_EXP_MSTRKEY_11239.CSV",
            "CAC_EXP_MSTRKEY_12822_2024.CSV",
            "CAC_EXP_MSTRKEY_12883_2024.CSV",
            "CAC_EXP_MSTRKEY_13152_2024.CSV",
            "CAC_EXP_MSTRKEY_13504_2024.CSV",
            "CAC_EXP_MSTRKEY_13596_2024.CSV",
            "CAC_EXP_MSTRKEY_14779_2024.CSV",
            "CAC_EXP_MSTRKEY_14814_2024.CSV",
            "CAC_EXP_MSTRKEY_14817_2024.CSV",
            "CAC_EXP_MSTRKEY_15242_2024.CSV",
            "CAC_EXP_MSTRKEY_15259_2024.CSV",
            "CAC_EXP_MSTRKEY_15838_2024.CSV",
            "CAC_EXP_MSTRKEY_15904_2024.CSV",
            "CAC_EXP_MSTRKEY_583_2024.CSV",
            "CAC_EXP_MSTRKEY_6541_2024.CSV",
            "CAC_EXP_MSTRKEY_7400_2024.CSV",
            "PPC_CON_MSTRKEY_15460_2024.CSV",
            "PPC_CON_MSTRKEY_1871_2024.CSV",
            "PPC_CON_MSTRKEY_1934_2024.CSV",
            "PPC_CON_MSTRKEY_2112_2024.CSV",
            "PPC_CON_MSTRKEY_5001_2024.CSV",
            "PPC_CON_MSTRKEY_5006_2024.CSV",
            "PPC_CON_MSTRKEY_6167_2024.CSV",
            "PPC_CON_MSTRKEY_6168_2024.CSV",
            "PPC_EXP_MSTRKEY_15460_2024.CSV",
            "PPC_EXP_MSTRKEY_1871_2024.CSV",
            "PPC_EXP_MSTRKEY_1934_2024.CSV",
            "PPC_EXP_MSTRKEY_2112_2024.CSV",
            "PPC_EXP_MSTRKEY_5001_2024.CSV",
            "PPC_EXP_MSTRKEY_5006_2024.CSV",
            "PPC_EXP_MSTRKEY_6167_2024.CSV",
            "PPC_EXP_MSTRKEY_6168_2024.CSV",
        ],
        "pac_files": ["ACT_PAC_LIST.CSV", "PAC_COV.CSV"],
        "party_files": [
            "PAR_CON_MSTRKEY_1821.CSV",
            "PAR_CON_MSTRKEY_1871.CSV",
            "PAR_CON_MSTRKEY_1934.CSV",
            "PAR_CON_MSTRKEY_2112.CSV",
            "PAR_CON_MSTRKEY_5001.CSV",
            "PAR_CON_MSTRKEY_5006.CSV",
            "PAR_CON_MSTRKEY_6167.CSV",
            "PAR_CON_MSTRKEY_6168.CSV",
            "PAR_COVER.CSV",
            "PAR_EXP_MSTRKEY_1821.CSV",
            "PAR_EXP_MSTRKEY_1871.CSV",
            "PAR_EXP_MSTRKEY_1934.CSV",
            "PAR_EXP_MSTRKEY_2112.CSV",
            "PAR_EXP_MSTRKEY_5001.CSV",
            "PAR_EXP_MSTRKEY_5006.CSV",
            "PAR_EXP_MSTRKEY_6167.CSV",
            "PAR_EXP_MSTRKEY_6168.CSV",
            "PPC_CON_MSTRKEY_15460.CSV",
            "PPC_EXP_MSTRKEY_15460.CSV",
        ],
    },
}


@dg.asset(name="oh_download_all_data")
def oh_download_all_data(context: AssetExecutionContext) -> list[Path]:
    """
    Downloads all Ohio campaign finance data files from Supabase storage.
    Returns a flat list of Path objects for all downloaded files.
    """
    downloaded_files = []
    logger = context.log  # Get Dagster logger from context

    for category in FILE_CONFIG["patterns"]:
        category_dir = Path(OH_DATA_BASE_PATH) / category
        category_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"🚀 Starting processing for category: {category.upper()}")

        # Download pattern-based files
        for pattern in FILE_CONFIG["patterns"][category]:
            logger.debug(f"Processing pattern: {pattern}")
            for year in YEARS:
                filename = f"{pattern}{year}.CSV"
                output_path = category_dir / filename
                download_url = f"{OH_SUPABASE_STORAGE_URL}{category}/{filename}"

                if output_path.exists():
                    logger.info(f"⏩ Skipping existing file: {filename}")
                    downloaded_files.append(output_path)
                    continue

                try:
                    logger.info(f"⬇️ Starting download: {filename}")
                    stream_download_file_to_path(
                        request_url=download_url, file_save_path=output_path
                    )
                    logger.info(f"✅ Successfully downloaded: {filename}")
                    downloaded_files.append(output_path)
                except Exception as e:
                    logger.warning(f"⚠️ {filename} download file not found!", exc_info=e)
                    logger.warning("🔄 Continuing with next file...")
                    continue

        # Download static files
        static_files = FILE_CONFIG["static_files"].get(category, [])
        logger.info(f"📦 Processing {len(static_files)} static files for {category}")

        for filename in static_files:
            output_path = category_dir / filename
            download_url = f"{OH_SUPABASE_STORAGE_URL}{category}/{filename}"

            if output_path.exists():
                logger.info(f"⏩ Skipping existing static file: {filename}")
                downloaded_files.append(output_path)
                continue

            try:
                logger.info(f"⬇️ Starting static file download: {filename}")
                stream_download_file_to_path(
                    request_url=download_url, file_save_path=output_path
                )
                logger.info(f"✅ Successfully downloaded static file: {filename}")
                downloaded_files.append(output_path)
            except Exception as e:
                logger.warning(
                    f"⚠️ {filename} static download file not found! ", exc_info=e
                )
                logger.warning("🔄 Continuing with next file...")
                continue

        logger.info(f"🏁 Completed processing for category: {category.upper()}")

    logger.info(
        f"🎉 All downloads completed! Total files processed: {len(downloaded_files)}"
    )
    return downloaded_files


table_schemas = {
    "CAN_CON": {
        "table_name": "oh_candidate_contribution_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "REPORT_DESCRIPTION",
            "RPT_YEAR",
            "REPORT_KEY",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "PAC_REG_NO",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "FILE_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "EMP_OCCUPATION",
            "INKIND_DESCRIPTION",
            "OTHER_INCOME_TYPE",
            "RCV_EVENT",
            "CANDIDATE_FIRST_NAME",
            "CANDIDATE_LAST_NAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
        ],
    },
    "CAC_CON": {
        "table_name": "oh_candidate_contribution_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "REPORT_DESCRIPTION",
            "RPT_YEAR",
            "REPORT_KEY",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "PAC_REG_NO",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "FILE_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "EMP_OCCUPATION",
            "INKIND_DESCRIPTION",
            "OTHER_INCOME_TYPE",
            "RCV_EVENT",
            "CANDIDATE_FIRST_NAME",
            "CANDIDATE_LAST_NAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
        ],
    },
    "CAN_EXP": {
        "table_name": "oh_candidate_expenditure_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "EXPEND_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "PURPOSE",
            "INKIND",
            "CANDIDATE FIRST NAME",
            "CANDIDATE LAST NAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
        ],
    },
    "CAC_EXP": {
        "table_name": "oh_candidate_expenditure_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "EXPEND_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "PURPOSE",
            "INKIND",
            "CANDIDATE FIRST NAME",
            "CANDIDATE LAST NAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
        ],
    },
    "ACT_CAN_LIST": {
        "table_name": "oh_candidate_list_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "COM_ADDRESS",
            "COM_CITY",
            "COM_STATE",
            "COM_ZIP",
            "TREA_FIRST_NAME",
            "TREA_LAST_NAME",
            "TREA_MIDDLE_NAME",
            "TREA_SUFFIX",
            "TREA_ADDRESS",
            "TREA_CITY",
            "TREA_STATE",
            "TREA_ZIP",
            "DEP_FIRST_NAME",
            "DEP_LAST_NAME",
            "CANDIDATE_FIRST_NAME",
            "CANDIDATE_LAST_NAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
            "SPONSOR",
        ],
    },
    "CAN_COVER": {
        "table_name": "oh_candidate_cover_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "CANDIDATE_FIRST_NAME",
            "CANDIDATE_LAST_NAME",
            "REPORT_KEY",
            "RPT_YEAR",
            "REPORT_DESCRIPTION",
            "DATE_REPORT_FILED",
            "AMT_FORWARD",
            "TOTAL_CONTRIBUTIONS",
            "TOTAL_OTHER_INCOME",
            "TOTAL_FUNDS",
            "TOTAL_EXPENDITURES",
            "BALANCE_ON_HAND",
            "VALUE_INKIND_RECEIVED",
            "VALUE_INKIND_MADE",
            "OUTSTANDING_LOANS_OWED",
            "OUTSTANDING_DEBT_OWED",
            "OUTSTANDING_LOANS_TO",
            "VALUE_IND_EXPENDITURES",
        ],
    },
    # end of candidate
    # Start of party actions (pac) files
    "PAC_CON": {
        "table_name": "oh_committee_contribution_landing",
        "schema": [
            "COM_NAME",
            "PAC_REG_NO",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "FILE_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "EMP_OCCUPATION",
            "INKIND_DESCRIPTION",
            "OTHER_INCOME_TYPE",
            "RCV_EVENT",
        ],
    },
    "PAC_EXP": {
        "table_name": "oh_committee_expenditure_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "EXPEND_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "PURPOSE",
        ],
    },
    "ACT_PAC_LIST": {
        "table_name": "oh_committee_list_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "COM_ADDRESS",
            "COM_CITY",
            "COM_STATE",
            "COM_ZIP",
            "PAC_REG_NO",
            "TREA_FIRST_NAME",
            "TREA_LAST_NAME",
            "TREA_MIDDLE_NAME",
            "TREA_SUFFIX",
            "TREA_ADDRESS",
            "TREA_CITY",
            "TREA_STATE",
            "TREA_ZIP",
            "DEP_FIRST_NAME",
            "DEP_LAST_NAME",
            "SPONSOR",
        ],
    },
    "PAC_COV": {
        "table_name": "oh_committee_cover_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "REPORT_KEY",
            "RPT_YEAR",
            "REPORT_DESCRIPTION",
            "DATE_REPORT_FILED",
            "PAC_REG_NO",
            "AMT_FORWARD",
            "TOTAL_CONTRIBUTIONS",
            "TOTAL_OTHER_INCOME",
            "TOTAL_FUNDS",
            "TOTAL_EXPENDITURES",
            "BALANCE_ON_HAND",
            "VALUE_INKIND_RECEIVED",
            "VALUE_INKIND_MADE",
            "OUTSTANDING_LOANS_OWED",
            "OUTSTANDING_DEBT_OWED",
            "OUTSTANDING_LOANS_TO",
            "VALUE_IND_EXPENDITURES",
        ],
    },
    # end of pac files
    # Start of party files
    "PAR_CON": {
        "table_name": "oh_party_contribution_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "PAC_REG_NO",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "FILE_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "EMP_OCCUPATION",
            "INKIND_DESCRIPTION",
            "OTHER_INCOME_TYPE",
            "RCV_EVENT",
        ],
    },
    "PPC_CON": {
        "table_name": "oh_party_contribution_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "PAC_REG_NO",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "FILE_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "EMP_OCCUPATION",
            "INKIND_DESCRIPTION",
            "OTHER_INCOME_TYPE",
            "RCV_EVENT",
        ],
    },
    "PAR_EXP": {
        "table_name": "oh_party_expenditure_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "EXPEND_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "PURPOSE",
            "PARTY",
        ],
    },
    "PPC_EXP": {
        "table_name": "oh_party_expenditure_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "RPT_YEAR",
            "REPORT_KEY",
            "REPORT_DESCRIPTION",
            "SHORT_DESCRIPTION",
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
            "SUFFIX_NAME",
            "NON_INDIVIDUAL",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP",
            "EXPEND_DATE",
            "AMOUNT",
            "EVENT_DATE",
            "PURPOSE",
            "PARTY",
        ],
    },
    "PAR_COVER": {
        "table_name": "oh_party_cover_landing",
        "schema": [
            "COM_NAME",
            "MASTER_KEY",
            "REPORT_KEY",
            "RPT_YEAR",
            "REPORT_DESCRIPTION",
            "DATE_REPORT_FILED",
            "AMT_FORWARD",
            "TOTAL_CONTRIBUTIONS",
            "TOTAL_OTHER_INCOME",
            "TOTAL_FUNDS",
            "TOTAL_EXPENDITURES",
            "BALANCE_ON_HAND",
            "VALUE_INKIND_RECEIVED",
            "VALUE_INKIND_MADE",
            "OUTSTANDING_LOANS_OWED",
            "OUTSTANDING_DEBT_OWED",
            "OUTSTANDING_LOANS_TO",
            "VALUE_IND_EXPENDITURES",
        ],
    },
}


def get_schema_by_filename(filename):
    for key in table_schemas:
        if key in filename:
            return {
                "matched_key": key,
                "table_name": table_schemas[key]["table_name"],
                "schema": table_schemas[key]["schema"],
            }
    return None


def process_table_files(
    context: AssetExecutionContext,
    pg: PostgresResource,
    files: list[Path],
    table_keys: list[str],
    table_name: str,
):
    """
    Generic processor for loading files into specific landing tables
    """
    processed_files = 0
    skipped_files = 0
    errors = 0
    truncate_query = get_sql_truncate_query(table_name=table_name)

    with pg.pool.connection() as conn, conn.cursor() as cursor:
        context.log.info("Truncating table before inserting new data")
        cursor.execute(truncate_query)

        for file_path in files:
            file_name = file_path.name
            context.log.info(f"Processing {file_name}")

            # Skip files not matching our target keys
            if not any(key in file_name for key in table_keys):
                skipped_files += 1
                continue

            # Get schema information
            schema_info = get_schema_by_filename(file_name)
            if not schema_info or schema_info["table_name"] != table_name:
                context.log.warning(f"Skipping {file_name} - Schema mismatch")
                skipped_files += 1
                continue

            try:
                cursor = conn.cursor()
                # Read and parse file
                file_lines = safe_readline_csv_like_file(file_path)
                parsed_file = csv.reader(file_lines, delimiter=",", quotechar='"')
                next(parsed_file, None)  # Skip header

                # Insert data
                insert_parsed_file_to_landing_table(
                    pg_cursor=cursor,
                    csv_reader=parsed_file,
                    table_name=table_name,
                    table_columns_name=schema_info["schema"],
                    row_validation_callback=lambda row, schema=schema_info: (
                        len(row) == len(schema["schema"])
                    ),
                )
                processed_files += 1
                context.log.info(f"Successfully processed {file_name}")

            except Exception as e:
                errors += 1
                conn.rollback()
                context.log.error(f"Failed to process {file_name}: {e!s}")
                continue

    context.log.info(
        f"Completed processing for {table_name}: "
        f"{processed_files} successful, {skipped_files} skipped, {errors} errors"
    )


@dg.asset(name="oh_insert_candidate_contributions", deps=[oh_download_all_data])
def oh_insert_candidate_contributions(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process candidate contribution files (CAN_CON, CAC_CON)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["CAN_CON", "CAC_CON"],
        table_name="oh_candidate_contribution_landing",
    )


@dg.asset(name="oh_insert_candidate_expenditures", deps=[oh_download_all_data])
def oh_insert_candidate_expenditures(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process candidate expenditure files (CAN_EXP, CAC_EXP)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["CAN_EXP", "CAC_EXP"],
        table_name="oh_candidate_expenditure_landing",
    )


@dg.asset(name="oh_insert_candidate_lists", deps=[oh_download_all_data])
def oh_insert_candidate_lists(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process candidate list files (ACT_CAN_LIST)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["ACT_CAN_LIST"],
        table_name="oh_candidate_list_landing",
    )


@dg.asset(name="oh_insert_candidate_covers", deps=[oh_download_all_data])
def oh_insert_candidate_covers(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process candidate cover files (CAN_COVER)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["CAN_COVER"],
        table_name="oh_candidate_cover_landing",
    )


@dg.asset(name="oh_insert_committee_contributions", deps=[oh_download_all_data])
def oh_insert_committee_contributions(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process committee contribution files (PAC_CON)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAC_CON"],
        table_name="oh_committee_contribution_landing",
    )


@dg.asset(name="oh_insert_committee_expenditures", deps=[oh_download_all_data])
def oh_insert_committee_expenditures(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process committee expenditure files (PAC_EXP)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAC_EXP"],
        table_name="oh_committee_expenditure_landing",
    )


@dg.asset(name="oh_insert_committee_lists", deps=[oh_download_all_data])
def oh_insert_committee_lists(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process committee list files (ACT_PAC_LIST)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["ACT_PAC_LIST"],
        table_name="oh_committee_list_landing",
    )


@dg.asset(name="oh_insert_committee_covers", deps=[oh_download_all_data])
def oh_insert_committee_covers(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process committee cover files (PAC_COV)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAC_COV"],
        table_name="oh_committee_cover_landing",
    )


@dg.asset(name="oh_insert_party_contributions", deps=[oh_download_all_data])
def oh_insert_party_contributions(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process party contribution files (PAR_CON, PPC_CON)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAR_CON", "PPC_CON"],
        table_name="oh_party_contribution_landing",
    )


@dg.asset(name="oh_insert_party_expenditures", deps=[oh_download_all_data])
def oh_insert_party_expenditures(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process party expenditure files (PAR_EXP, PPC_EXP)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAR_EXP", "PPC_EXP"],
        table_name="oh_party_expenditure_landing",
    )


@dg.asset(name="oh_insert_party_covers", deps=[oh_download_all_data])
def oh_insert_party_covers(
    context: AssetExecutionContext,
    pg: dg.ResourceParam[PostgresResource],
    oh_download_all_data: list[Path],
):
    """Process party cover files (PAR_COVER)"""
    process_table_files(
        context=context,
        pg=pg,
        files=oh_download_all_data,
        table_keys=["PAR_COVER"],
        table_name="oh_party_cover_landing",
    )

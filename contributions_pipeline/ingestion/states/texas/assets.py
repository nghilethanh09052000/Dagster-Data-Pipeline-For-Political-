import csv
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

TX_DATA_PATH_PREFIX = "./states/texas"
TX_DATA_DOWNLOAD_URL = (
    "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip"
)


CATEGORY_MAP = [
    "assets",
    "cand",
    "cont_ss",
    "cont_t",
    "contribs",
    "cover",
    "credits",
    "debts",
    "expend",
    "expn_catg",
    "expn_t",
    "filers",
    "finals",
    "loans",
    "notices",
    "pledges",
    "purpose",
    "spacs",
    "travel",
]


def tx_insert_to_landing_table(
    postgres: ConnectionPool,
    table_name: str,
    folder_name: str,
    columns: list[str],
    data_validation_callback: Callable[[list[str]], bool],
):
    """
    Inserts data from CSV files into the specified landing table
    in a PostgreSQL database.

    Args:
        - postgres (ConnectionPool): The database connection pool
                used to interact with the PostgreSQL database.
        - table_name (str): The name of the landing table where data should be inserted.
        - folder_name (str): The folder name within the
                            TX_DATA_PATH_PREFIX that contains the CSV files.
        - columns (list[str]): A list of column names for
                                the table into which data will be inserted.
        - data_validation_callback (Callable[[list[str]], bool]):
                            A callback function used for validating each row
                            of data before insertion. The callback should accept
                            a list of column values and return a boolean
                            indicating whether the row is valid.

    Raises:
        Exception: If an error occurs while reading a CSV file,
                    processing data, or inserting data into the table,
                    an exception will be raised.
    """
    logger = dg.get_dagster_logger(name=f"{table_name}_insert")

    data_dir = f"{TX_DATA_PATH_PREFIX}/{folder_name}/"

    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

    if not files:
        logger.warning(f"No CSV files found in {data_dir}, skipping insertion.")
        return

    logger.info(f"Processing category: {table_name}, Files: {files}")

    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name} before inserting new data.")
        pg_cursor.execute(query=truncate_query)

        for file_name in files:
            data_path = os.path.join(data_dir, file_name)
            logger.info(f"Processing file: {data_path}")

            try:
                data_type_lines_generator = safe_readline_csv_like_file(
                    data_path,
                    encoding="utf-8",
                )

                parsed_data_type_file = csv.reader(
                    data_type_lines_generator, delimiter=","
                )

                next(parsed_data_type_file)

                insert_parsed_file_to_landing_table(
                    pg_cursor=pg_cursor,
                    csv_reader=parsed_data_type_file,
                    table_name=table_name,
                    table_columns_name=columns,
                    row_validation_callback=data_validation_callback,
                )

            except Exception as e:
                logger.error(f"Error while processing {data_path}: {e}")
                raise e

        count_query = get_sql_count_query(table_name=table_name)
        count_cursor_result = pg_cursor.execute(query=count_query).fetchone()
        row_count = (
            int(count_cursor_result[0]) if count_cursor_result is not None else 0
        )

        logger.info(
            f"Successfully inserted \
            {row_count} rows into {table_name}"
        )

        return dg.MaterializeResult(
            metadata={
                "dagster/table_name": table_name,
                "dagster/row_count": row_count,
            }
        )


@dg.asset(
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        # Delay for 20 minutes for each time there's error
        delay=30 * 60,
        # attempt_num * delay (so should be 60m wait maximum)
        backoff=dg.Backoff.LINEAR,
    )
)
def tx_fetch_zip_data() -> None:
    """
    Fetches and extracts zip data for Texas campaign finance,
    categorizing CSV files into subdirectories.

    This function:
    1. Downloads a zip file from a given URL (`BASED_URL`)
        to a specified directory (`TX_DATA_PATH_PREFIX`)
    2. Extracts the zip file, searching for `.csv` files
        and categorizing them based on predefined categories
        in `CATEGORY_MAP`.
    3. Saves each `.csv` file into a corresponding subdirectory
        (based on its category) within the base directory.
    4. Removes the original zip file after extraction is complete.


    **Side Effects**:
    - Downloads the zip file to the local file system.
    - Extracts and saves CSV files into categorized subdirectories.
    - Deletes the downloaded zip file after extraction.

    """
    logger = dg.get_dagster_logger(name="tx_fetch_zip_data")
    logger.info("Fetching zip data for Texas...")

    base_dir = Path(TX_DATA_PATH_PREFIX)
    base_dir.mkdir(parents=True, exist_ok=True)
    zip_file_path = base_dir / "texas_data.zip"

    stream_download_file_to_path(
        request_url=TX_DATA_DOWNLOAD_URL,
        file_save_path=zip_file_path,
    )

    try:
        with ZipFile(zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith(".csv"):
                    for prefix in CATEGORY_MAP:
                        if file_info.filename.startswith(prefix):
                            category = prefix
                            break

                    category_dir = base_dir / category
                    category_dir.mkdir(parents=True, exist_ok=True)

                    extracted_file_path = category_dir / file_info.filename
                    with (
                        zip_ref.open(file_info) as source,
                        open(extracted_file_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)

        logger.info("Extraction and categorization complete.")

    finally:
        os.remove(zip_file_path)


@dg.asset(deps=[tx_fetch_zip_data])
def tx_assets_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_assets_landing",
        folder_name="assets",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "asset_info_id",
            "asset_descr",
        ],
        data_validation_callback=lambda row: len(row) == 11,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_cand_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_cand_landing",
        folder_name="cand",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "expend_info_id",
            "expend_persent_id",
            "expend_dt",
            "expend_amount",
            "expend_descr",
            "expend_cat_cd",
            "expend_cat_descr",
            "itemize_flag",
            "political_expend_cd",
            "reimburse_intended_flag",
            "src_corp_contrib_flag",
            "capital_livingexp_flag",
            "candidate_persent_type_cd",
            "candidate_name_organization",
            "candidate_name_last",
            "candidate_name_suffix_cd",
            "candidate_name_first",
            "candidate_name_prefix_cd",
            "candidate_name_short",
            "candidate_hold_office_cd",
            "candidate_hold_office_district",
            "candidate_hold_office_place",
            "candidate_hold_office_descr",
            "candidate_hold_office_county_cd",
            "candidate_hold_office_county_descr",
            "candidate_seek_office_cd",
            "candidate_seek_office_district",
            "candidate_seek_office_place",
            "candidate_seek_office_descr",
            "candidate_seek_office_county_cd",
            "candidate_seek_office_county_descr",
        ],
        data_validation_callback=lambda row: len(row) == 40,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_cont_ss_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_cont_ss_landing",
        folder_name="cont_ss",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "contribution_info_id",
            "contribution_dt",
            "contribution_amount",
            "contribution_descr",
            "itemize_flag",
            "travel_flag",
            "contributor_persent_type_cd",
            "contributor_name_organization",
            "contributor_name_last",
            "contributor_name_suffix_cd",
            "contributor_name_first",
            "contributor_name_prefix_cd",
            "contributor_name_short",
            "contributor_street_city",
            "contributor_street_state_cd",
            "contributor_street_county_cd",
            "contributor_street_country_cd",
            "contributor_street_postal_code",
            "contributor_street_region",
            "contributor_employer",
            "contributor_occupation",
            "contributor_job_title",
            "contributor_pac_fein",
            "contributor_oos_pac_flag",
            "contributor_law_firm_name",
            "contributor_spouse_law_firm_name",
            "contributor_parent1_law_firm_name",
            "contributor_parent2_law_firm_name",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_cont_t_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_cont_t_landing",
        folder_name="cont_t",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "contribution_info_id",
            "contribution_dt",
            "contribution_amount",
            "contribution_descr",
            "itemize_flag",
            "travel_flag",
            "contributor_persent_type_cd",
            "contributor_name_organization",
            "contributor_name_last",
            "contributor_name_suffix_cd",
            "contributor_name_first",
            "contributor_name_prefix_cd",
            "contributor_name_short",
            "contributor_street_city",
            "contributor_street_state_cd",
            "contributor_street_county_cd",
            "contributor_street_country_cd",
            "contributor_street_postal_code",
            "contributor_street_region",
            "contributor_employer",
            "contributor_occupation",
            "contributor_job_title",
            "contributor_pac_fein",
            "contributor_oos_pac_flag",
            "contributor_law_firm_name",
            "contributor_spouse_law_firm_name",
            "contributor_parent1_law_firm_name",
            "contributor_parent2_law_firm_name",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_contribs_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_contribs_landing",
        folder_name="contribs",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "contribution_info_id",
            "contribution_dt",
            "contribution_amount",
            "contribution_descr",
            "itemize_flag",
            "travel_flag",
            "contributor_persent_type_cd",
            "contributor_name_organization",
            "contributor_name_last",
            "contributor_name_suffix_cd",
            "contributor_name_first",
            "contributor_name_prefix_cd",
            "contributor_name_short",
            "contributor_street_city",
            "contributor_street_state_cd",
            "contributor_street_county_cd",
            "contributor_street_country_cd",
            "contributor_street_postal_code",
            "contributor_street_region",
            "contributor_employer",
            "contributor_occupation",
            "contributor_job_title",
            "contributor_pac_fein",
            "contributor_oos_pac_flag",
            "contributor_law_firm_name",
            "contributor_spouse_law_firm_name",
            "contributor_parent1_law_firm_name",
            "contributor_parent2_law_firm_name",
        ],
        data_validation_callback=lambda row: True,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_cover_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_cover_landing",
        folder_name="cover",
        columns=[
            "record_type",
            "form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "report_type_cd1",
            "report_type_cd2",
            "report_type_cd3",
            "report_type_cd4",
            "report_type_cd5",
            "report_type_cd6",
            "report_type_cd7",
            "report_type_cd8",
            "report_type_cd9",
            "report_type_cd10",
            "source_category_cd",
            "due_dt",
            "filed_dt",
            "period_start_dt",
            "period_end_dt",
            "unitemized_contrib_amount",
            "total_contrib_amount",
            "unitemized_expend_amount",
            "total_expend_amount",
            "loan_balance_amount",
            "contribs_maintained_amount",
            "unitemized_pledge_amount",
            "unitemized_loan_amount",
            "total_interest_earned_amount",
            "election_dt",
            "election_type_cd",
            "election_type_descr",
            "no_activity_flag",
            "political_party_cd",
            "political_division_cd",
            "political_party_other_descr",
            "political_party_county_cd",
            "timely_correction_flag",
            "semiannual_checkbox_flag",
            "high_contrib_threshhold_cd",
            "software_release",
            "internet_visible_flag",
            "signer_printed_name",
            "addr_change_filer_flag",
            "addr_change_treas_flag",
            "addr_change_chair_flag",
            "filer_persent_type_cd",
            "filer_name_organization",
            "filer_name_last",
            "filer_name_suffix_cd",
            "filer_name_first",
            "filer_name_prefix_cd",
            "filer_name_short",
            "filer_street_addr1",
            "filer_street_addr2",
            "filer_street_city",
            "filer_street_state_cd",
            "filer_street_county_cd",
            "filer_street_country_cd",
            "filer_street_postal_code",
            "filer_street_region",
            "filer_hold_office_cd",
            "filer_hold_office_district",
            "filer_hold_office_place",
            "filer_hold_office_descr",
            "filer_hold_office_county_cd",
            "filer_hold_office_county_descr",
            "filer_seek_office_cd",
            "filer_seek_office_district",
            "filer_seek_office_place",
            "filer_seek_office_descr",
            "filer_seek_office_county_cd",
            "filer_seek_office_county_descr",
            "treas_persent_type_cd",
            "treas_name_organization",
            "treas_name_last",
            "treas_name_suffix_cd",
            "treas_name_first",
            "treas_name_prefix_cd",
            "treas_name_short",
            "treas_street_addr1",
            "treas_street_addr2",
            "treas_street_city",
            "treas_street_state_cd",
            "treas_street_county_cd",
            "treas_street_country_cd",
            "treas_street_postal_code",
            "treas_street_region",
            "treas_mailing_addr1",
            "treas_mailing_addr2",
            "treas_mailing_city",
            "treas_mailing_state_cd",
            "treas_mailing_county_cd",
            "treas_mailing_country_cd",
            "treas_mailing_postal_code",
            "treas_mailing_region",
            "treas_primary_usa_phone_flag",
            "treas_primary_phone_number",
            "treas_primary_phone_ext",
            "chair_persent_type_cd",
            "chair_name_organization",
            "chair_name_last",
            "chair_name_suffix_cd",
            "chair_name_first",
            "chair_name_prefix_cd",
            "chair_name_short",
            "chair_street_addr1",
            "chair_street_addr2",
            "chair_street_city",
            "chair_street_state_cd",
            "chair_street_county_cd",
            "chair_street_country_cd",
            "chair_street_postal_code",
            "chair_street_region",
            "chair_mailing_addr1",
            "chair_mailing_addr2",
            "chair_mailing_city",
            "chair_mailing_state_cd",
            "chair_mailing_county_cd",
            "chair_mailing_country_cd",
            "chair_mailing_postal_code",
            "chair_mailing_region",
            "chair_primary_usa_phone_flag",
            "chair_primary_phone_number",
            "chair_primary_phone_ext",
        ],
        data_validation_callback=lambda row: len(row) == 128,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_credits_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_credits_landing",
        folder_name="credits",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "credit_info_id",
            "credit_dt",
            "credit_amount",
            "credit_descr",
            "payor_persent_type_cd",
            "payor_name_organization",
            "payor_name_last",
            "payor_name_suffix_cd",
            "payor_name_first",
            "payor_name_prefix_cd",
            "payor_name_short",
            "payor_street_addr1",
            "payor_street_addr2",
            "payor_street_city",
            "payor_street_state_cd",
            "payor_street_county_cd",
            "payor_street_country_cd",
            "payor_street_postal_code",
            "payor_street_region",
        ],
        data_validation_callback=lambda row: len(row) == 28,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_debts_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_debts_landing",
        folder_name="debts",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "loan_info_id",
            "loan_guaranteed_flag",
            "lender_persent_type_cd",
            "lender_name_organization",
            "lender_name_last",
            "lender_name_suffix_cd",
            "lender_name_first",
            "lender_name_prefix_cd",
            "lender_name_short",
            "lender_street_city",
            "lender_street_state_cd",
            "lender_street_county_cd",
            "lender_street_country_cd",
            "lender_street_postal_code",
            "lender_street_region",
            "guarantor_persent_type_cd1",
            "guarantor_name_organization1",
            "guarantor_name_last1",
            "guarantor_name_suffix_cd1",
            "guarantor_name_first1",
            "guarantor_name_prefix_cd1",
            "guarantor_name_short1",
            "guarantor_street_city1",
            "guarantor_street_state_cd1",
            "guarantor_street_county_cd1",
            "guarantor_street_country_cd1",
            "guarantor_street_postal_code1",
            "guarantor_street_region1",
            "guarantor_persent_type_cd2",
            "guarantor_name_organization2",
            "guarantor_name_last2",
            "guarantor_name_suffix_cd2",
            "guarantor_name_first2",
            "guarantor_name_prefix_cd2",
            "guarantor_name_short2",
            "guarantor_street_city2",
            "guarantor_street_state_cd2",
            "guarantor_street_county_cd2",
            "guarantor_street_country_cd2",
            "guarantor_street_postal_code2",
            "guarantor_street_region2",
            "guarantor_persent_type_cd3",
            "guarantor_name_organization3",
            "guarantor_name_last3",
            "guarantor_name_suffix_cd3",
            "guarantor_name_first3",
            "guarantor_name_prefix_cd3",
            "guarantor_name_short3",
            "guarantor_street_city3",
            "guarantor_street_state_cd3",
            "guarantor_street_county_cd3",
            "guarantor_street_country_cd3",
            "guarantor_street_postal_code3",
            "guarantor_street_region3",
            "guarantor_persent_type_cd4",
            "guarantor_name_organization4",
            "guarantor_name_last4",
            "guarantor_name_suffix_cd4",
            "guarantor_name_first4",
            "guarantor_name_prefix_cd4",
            "guarantor_name_short4",
            "guarantor_street_city4",
            "guarantor_street_state_cd4",
            "guarantor_street_county_cd4",
            "guarantor_street_country_cd4",
            "guarantor_street_postal_code4",
            "guarantor_street_region4",
            "guarantor_persent_type_cd5",
            "guarantor_name_organization5",
            "guarantor_name_last5",
            "guarantor_name_suffix_cd5",
            "guarantor_name_first5",
            "guarantor_name_prefix_cd5",
            "guarantor_name_short5",
            "guarantor_street_city5",
            "guarantor_street_state_cd5",
            "guarantor_street_county_cd5",
            "guarantor_street_country_cd5",
            "guarantor_street_postal_code5",
            "guarantor_street_region5",
        ],
        data_validation_callback=lambda row: len(row) == 89,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_expend_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_expend_landing",
        folder_name="expend",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "expend_info_id",
            "expend_dt",
            "expend_amount",
            "expend_descr",
            "expend_cat_cd",
            "expend_cat_descr",
            "itemize_flag",
            "travel_flag",
            "political_expend_cd",
            "reimburse_intended_flag",
            "src_corp_contrib_flag",
            "capital_livingexp_flag",
            "payee_persent_type_cd",
            "payee_name_organization",
            "payee_name_last",
            "payee_name_suffix_cd",
            "payee_name_first",
            "payee_name_prefix_cd",
            "payee_name_short",
            "payee_street_addr1",
            "payee_street_addr2",
            "payee_street_city",
            "payee_street_state_cd",
            "payee_street_county_cd",
            "payee_street_country_cd",
            "payee_street_postal_code",
            "payee_street_region",
            "credit_card_issuer",
            "repayment_dt",
        ],
        data_validation_callback=lambda row: len(row) == 38,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_expn_catg_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_expn_catg_landing",
        folder_name="expn_catg",
        columns=[
            "record_type",
            "expend_category_code_value",
            "expend_category_code_label",
        ],
        data_validation_callback=lambda row: len(row) == 3,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_expn_t_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_expn_t_landing",
        folder_name="expn_t",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "expend_info_id",
            "expend_dt",
            "expend_amount",
            "expend_descr",
            "expend_cat_cd",
            "expend_cat_descr",
            "itemize_flag",
            "travel_flag",
            "political_expend_cd",
            "reimburse_intended_flag",
            "src_corp_contrib_flag",
            "capital_livingexp_flag",
            "payee_persent_type_cd",
            "payee_name_organization",
            "payee_name_last",
            "payee_name_suffix_cd",
            "payee_name_first",
            "payee_name_prefix_cd",
            "payee_name_short",
            "payee_street_addr1",
            "payee_street_addr2",
            "payee_street_city",
            "payee_street_state_cd",
            "payee_street_county_cd",
            "payee_street_country_cd",
            "payee_street_postal_code",
            "payee_street_region",
            "credit_card_issuer",
            "repayment_dt",
        ],
        data_validation_callback=lambda row: len(row) == 38,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_filers_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_filers_landing",
        folder_name="filers",
        columns=[
            "record_type",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "unexpend_contrib_filer_flag",
            "modified_elect_cycle_flag",
            "filer_jdi_cd",
            "committee_status_cd",
            "cta_seek_office_cd",
            "cta_seek_office_district",
            "cta_seek_office_place",
            "cta_seek_office_descr",
            "cta_seek_office_county_cd",
            "cta_seek_office_county_descr",
            "filer_persent_type_cd",
            "filer_name_organization",
            "filer_name_last",
            "filer_name_suffix_cd",
            "filer_name_first",
            "filer_name_prefix_cd",
            "filer_name_short",
            "filer_street_addr1",
            "filer_street_addr2",
            "filer_street_city",
            "filer_street_state_cd",
            "filer_street_county_cd",
            "filer_street_country_cd",
            "filer_street_postal_code",
            "filer_street_region",
            "filer_mailing_addr1",
            "filer_mailing_addr2",
            "filer_mailing_city",
            "filer_mailing_state_cd",
            "filer_mailing_county_cd",
            "filer_mailing_country_cd",
            "filer_mailing_postal_code",
            "filer_mailing_region",
            "filer_primary_usa_phone_flag",
            "filer_primary_phone_number",
            "filer_primary_phone_ext",
            "filer_hold_office_cd",
            "filer_hold_office_district",
            "filer_hold_office_place",
            "filer_hold_office_descr",
            "filer_hold_office_county_cd",
            "filer_hold_office_county_descr",
            "filer_filerpers_status_cd",
            "filer_eff_start_dt",
            "filer_eff_stop_dt",
            "contest_seek_office_cd",
            "contest_seek_office_district",
            "contest_seek_office_place",
            "contest_seek_office_descr",
            "contest_seek_office_county_cd",
            "contest_seek_office_county_descr",
            "treas_persent_type_cd",
            "treas_name_organization",
            "treas_name_last",
            "treas_name_suffix_cd",
            "treas_name_first",
            "treas_name_prefix_cd",
            "treas_name_short",
            "treas_street_addr1",
            "treas_street_addr2",
            "treas_street_city",
            "treas_street_state_cd",
            "treas_street_county_cd",
            "treas_street_country_cd",
            "treas_street_postal_code",
            "treas_street_region",
            "treas_mailing_addr1",
            "treas_mailing_addr2",
            "treas_mailing_city",
            "treas_mailing_state_cd",
            "treas_mailing_county_cd",
            "treas_mailing_country_cd",
            "treas_mailing_postal_code",
            "treas_mailing_region",
            "treas_primary_usa_phone_flag",
            "treas_primary_phone_number",
            "treas_primary_phone_ext",
            "treas_appointor_name_last",
            "treas_appointor_name_first",
            "treas_filerpers_status_cd",
            "treas_eff_start_dt",
            "treas_eff_stop_dt",
            "assttreas_persent_type_cd",
            "assttreas_name_organization",
            "assttreas_name_last",
            "assttreas_name_suffix_cd",
            "assttreas_name_first",
            "assttreas_name_prefix_cd",
            "assttreas_name_short",
            "assttreas_street_addr1",
            "assttreas_street_addr2",
            "assttreas_street_city",
            "assttreas_street_state_cd",
            "assttreas_street_county_cd",
            "assttreas_street_country_cd",
            "assttreas_street_postal_code",
            "assttreas_street_region",
            "assttreas_primary_usa_phone_flag",
            "assttreas_primary_phone_number",
            "assttreas_primary_phone_ext",
            "assttreas_appointor_name_last",
            "assttreas_appointor_name_first",
            "chair_persent_type_cd",
            "chair_name_organization",
            "chair_name_last",
            "chair_name_suffix_cd",
            "chair_name_first",
            "chair_name_prefix_cd",
            "chair_name_short",
            "chair_street_addr1",
            "chair_street_addr2",
            "chair_street_city",
            "chair_street_state_cd",
            "chair_street_county_cd",
            "chair_street_country_cd",
            "chair_street_postal_code",
            "chair_street_region",
            "chair_mailing_addr1",
            "chair_mailing_addr2",
            "chair_mailing_city",
            "chair_mailing_state_cd",
            "chair_mailing_county_cd",
            "chair_mailing_country_cd",
            "chair_mailing_postal_code",
            "chair_mailing_region",
            "chair_primary_usa_phone_flag",
            "chair_primary_phone_number",
            "chair_primary_phone_ext",
        ],
        data_validation_callback=lambda row: len(row) == 132,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_finals_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_finals_landing",
        folder_name="finals",
        columns=[
            "record_type",
            "form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "final_unexpend_contrib_flag",
            "final_retained_assets_flag",
            "final_officeholder_ack_flag",
        ],
        data_validation_callback=lambda row: len(row) == 11,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_loans_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_loans_landing",
        folder_name="loans",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "loan_info_id",
            "loan_dt",
            "loan_amount",
            "loan_descr",
            "interest_rate",
            "maturity_dt",
            "collateral_flag",
            "collateral_descr",
            "loan_status_cd",
            "payment_made_flag",
            "payment_amount",
            "payment_source",
            "loan_guaranteed_flag",
            "financial_institution_flag",
            "loan_guarantee_amount",
            "lender_persent_type_cd",
            "lender_name_organization",
            "lender_name_last",
            "lender_name_suffix_cd",
            "lender_name_first",
            "lender_name_prefix_cd",
            "lender_name_short",
            "lender_street_city",
            "lender_street_state_cd",
            "lender_street_county_cd",
            "lender_street_country_cd",
            "lender_street_postal_code",
            "lender_street_region",
            "lender_employer",
            "lender_occupation",
            "lender_job_title",
            "lender_pac_fein",
            "lender_oos_pac_flag",
            "lender_law_firm_name",
            "lender_spouse_law_firm_name",
            "lender_parent1_law_firm_name",
            "lender_parent2_law_firm_name",
            "guarantor_persent_type_cd1",
            "guarantor_name_organization1",
            "guarantor_name_last1",
            "guarantor_name_suffix_cd1",
            "guarantor_name_first1",
            "guarantor_name_prefix_cd1",
            "guarantor_name_short1",
            "guarantor_street_city1",
            "guarantor_street_state_cd1",
            "guarantor_street_county_cd1",
            "guarantor_street_country_cd1",
            "guarantor_street_postal_code1",
            "guarantor_street_region1",
            "guarantor_employer1",
            "guarantor_occupation1",
            "guarantor_job_title1",
            "guarantor_law_firm_name1",
            "guarantor_spouse_law_firm_name1",
            "guarantor_parent1_law_firm_name1",
            "guarantor_parent2_law_firm_name1",
            "guarantor_persent_type_cd2",
            "guarantor_name_organization2",
            "guarantor_name_last2",
            "guarantor_name_suffix_cd2",
            "guarantor_name_first2",
            "guarantor_name_prefix_cd2",
            "guarantor_name_short2",
            "guarantor_street_city2",
            "guarantor_street_state_cd2",
            "guarantor_street_county_cd2",
            "guarantor_street_country_cd2",
            "guarantor_street_postal_code2",
            "guarantor_street_region2",
            "guarantor_employer2",
            "guarantor_occupation2",
            "guarantor_job_title2",
            "guarantor_law_firm_name2",
            "guarantor_spouse_law_firm_name2",
            "guarantor_parent1_law_firm_name2",
            "guarantor_parent2_law_firm_name2",
            "guarantor_persent_type_cd3",
            "guarantor_name_organization3",
            "guarantor_name_last3",
            "guarantor_name_suffix_cd3",
            "guarantor_name_first3",
            "guarantor_name_prefix_cd3",
            "guarantor_name_short3",
            "guarantor_street_city3",
            "guarantor_street_state_cd3",
            "guarantor_street_county_cd3",
            "guarantor_street_country_cd3",
            "guarantor_street_postal_code3",
            "guarantor_street_region3",
            "guarantor_employer3",
            "guarantor_occupation3",
            "guarantor_job_title3",
            "guarantor_law_firm_name3",
            "guarantor_spouse_law_firm_name3",
            "guarantor_parent1_law_firm_name3",
            "guarantor_parent2_law_firm_name3",
            "guarantor_persent_type_cd4",
            "guarantor_name_organization4",
            "guarantor_name_last4",
            "guarantor_name_suffix_cd4",
            "guarantor_name_first4",
            "guarantor_name_prefix_cd4",
            "guarantor_name_short4",
            "guarantor_street_city4",
            "guarantor_street_state_cd4",
            "guarantor_street_county_cd4",
            "guarantor_street_country_cd4",
            "guarantor_street_postal_code4",
            "guarantor_street_region4",
            "guarantor_employer4",
            "guarantor_occupation4",
            "guarantor_job_title4",
            "guarantor_law_firm_name4",
            "guarantor_spouse_law_firm_name4",
            "guarantor_parent1_law_firm_name4",
            "guarantor_parent2_law_firm_name4",
            "guarantor_persent_type_cd5",
            "guarantor_name_organization5",
            "guarantor_name_last5",
            "guarantor_name_suffix_cd5",
            "guarantor_name_first5",
            "guarantor_name_prefix_cd5",
            "guarantor_name_short5",
            "guarantor_street_city5",
            "guarantor_street_state_cd5",
            "guarantor_street_county_cd5",
            "guarantor_street_country_cd5",
            "guarantor_street_postal_code5",
            "guarantor_street_region5",
            "guarantor_employer5",
            "guarantor_occupation5",
            "guarantor_job_title5",
            "guarantor_law_firm_name5",
            "guarantor_spouse_law_firm_name5",
            "guarantor_parent1_law_firm_name5",
            "guarantor_parent2_law_firm_name5",
        ],
        data_validation_callback=lambda row: len(row) == 146,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_notices_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_notices_landing",
        folder_name="notices",
        columns=[
            "record_type",
            "form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "committee_activity_id",
            "notifier_commact_persent_kind_cd",
            "notifier_persent_type_cd",
            "notifier_name_organization",
            "notifier_name_last",
            "notifier_name_suffix_cd",
            "notifier_name_first",
            "notifier_name_prefix_cd",
            "notifier_name_short",
            "notifier_street_addr1",
            "notifier_street_addr2",
            "notifier_street_city",
            "notifier_street_state_cd",
            "notifier_street_county_cd",
            "notifier_street_country_cd",
            "notifier_street_postal_code",
            "notifier_street_region",
            "treas_persent_type_cd",
            "treas_name_organization",
            "treas_name_last",
            "treas_name_suffix_cd",
            "treas_name_first",
            "treas_name_prefix_cd",
            "treas_name_short",
            "treas_street_addr1",
            "treas_street_addr2",
            "treas_street_city",
            "treas_street_state_cd",
            "treas_street_county_cd",
            "treas_street_country_cd",
            "treas_street_postal_code",
            "treas_street_region",
        ],
        data_validation_callback=lambda row: len(row) == 40,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_pledges_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_pledges_landing",
        folder_name="pledges",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "pledge_info_id",
            "pledge_dt",
            "pledge_amount",
            "pledge_descr",
            "itemize_flag",
            "travel_flag",
            "pledger_persent_type_cd",
            "pledger_name_organization",
            "pledger_name_last",
            "pledger_name_suffix_cd",
            "pledger_name_first",
            "pledger_name_prefix_cd",
            "pledger_name_short",
            "pledger_street_city",
            "pledger_street_state_cd",
            "pledger_street_county_cd",
            "pledger_street_country_cd",
            "pledger_street_postal_code",
            "pledger_street_region",
            "pledger_employer",
            "pledger_occupation",
            "pledger_job_title",
            "pledger_pac_fein",
            "pledger_oos_pac_flag",
            "pledger_law_firm_name",
            "pledger_spouse_law_firm_name",
            "pledger_parent1_law_firm_name",
            "pledger_parent2_law_firm_name",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_purpose_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_purpose_landing",
        folder_name="purpose",
        columns=[
            "record_type",
            "form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "committee_activity_id",
            "subject_category_cd",
            "subject_position_cd",
            "subject_descr",
            "subject_ballot_number",
            "subject_election_dt",
            "activity_hold_office_cd",
            "activity_hold_office_district",
            "activity_hold_office_place",
            "activity_hold_office_descr",
            "activity_hold_office_county_cd",
            "activity_hold_office_county_descr",
            "activity_seek_office_cd",
            "activity_seek_office_district",
            "activity_seek_office_place",
            "activity_seek_office_descr",
            "activity_seek_office_county_cd",
            "activity_seek_office_county_descr",
        ],
        data_validation_callback=lambda row: len(row) == 26,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_spacs_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_spacs_landing",
        folder_name="spacs",
        columns=[
            "record_type",
            "spac_filer_ident",
            "spac_filer_type_cd",
            "spac_filer_name",
            "spac_filer_name_short",
            "spac_committee_status_cd",
            "spac_treas_eff_start_dt",
            "spac_treas_eff_stop_dt",
            "spac_position_cd",
            "candidate_filer_ident",
            "candidate_filer_type_cd",
            "candidate_filer_name",
            "candidate_filerpers_status_cd",
            "candidate_eff_start_dt",
            "candidate_eff_stop_dt",
            "candidate_hold_office_cd",
            "candidate_hold_office_district",
            "candidate_hold_office_place",
            "candidate_hold_office_descr",
            "candidate_hold_office_county_cd",
            "candidate_hold_office_county_descr",
            "candidate_seek_office_cd",
            "candidate_seek_office_district",
            "candidate_seek_office_place",
            "candidate_seek_office_descr",
            "candidate_seek_office_county_cd",
            "candidate_seek_office_county_descr",
            "cta_seek_office_cd",
            "cta_seek_office_district",
            "cta_seek_office_place",
            "cta_seek_office_descr",
            "cta_seek_office_county_cd",
            "cta_seek_office_county_descr",
            "candtreas_filerpers_status_cd",
            "candtreas_eff_start_dt",
            "candtreas_eff_stop_dt",
        ],
        data_validation_callback=lambda row: len(row) == 36,
    )


@dg.asset(deps=[tx_fetch_zip_data])
def tx_travel_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    return tx_insert_to_landing_table(
        postgres=pg.pool,
        table_name="tx_travel_landing",
        folder_name="travel",
        columns=[
            "record_type",
            "form_type_cd",
            "sched_form_type_cd",
            "report_info_ident",
            "received_dt",
            "info_only_flag",
            "filer_ident",
            "filer_type_cd",
            "filer_name",
            "travel_info_id",
            "parent_type",
            "parent_id",
            "parent_dt",
            "parent_amount",
            "parent_full_name",
            "transportation_type_cd",
            "transportation_type_descr",
            "departure_city",
            "arrival_city",
            "departure_dt",
            "arrival_dt",
            "travel_purpose",
            "traveller_persent_type_cd",
            "traveller_name_organization",
            "traveller_name_last",
            "traveller_name_suffix_cd",
            "traveller_name_first",
            "traveller_name_prefix_cd",
            "traveller_name_short",
        ],
        data_validation_callback=lambda row: len(row) == 29,
    )

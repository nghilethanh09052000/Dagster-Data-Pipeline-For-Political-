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

CA_CAL_ACCESS_RAW_DATA_URL = "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"
CA_CAL_ACCESS_DATA_PREFIX = "CalAccess/DATA/"
CA_DATA_PATH_PREFIX = "./states/california"


def get_ca_raw_data_file_by_data_type(data_type: str) -> str:
    """
    Using the provided data type get the path of raw data.

    Parameters:
    data_type: upper case data type as provided by CAL-ACCESS, e.g. "DEBT", "S496", etc
    """
    return f"{CA_DATA_PATH_PREFIX}/{data_type}_CD.TSV"


def ca_insert_raw_file_to_landing_table(
    postgres_pool: ConnectionPool,
    data_type: str,
    table_name: str,
    table_columns_name: list[str],
    data_validation_callback: Callable[[list[str]], bool],
) -> dg.MaterializeResult:
    """
    Insert raw CAL-ACCESS data to landing table in Postgres.

    Parameters:
    connection_pool: postgres connection pool initialized by PostgresResource. Typical
                     use is case is probably getting it from dagster resource params.
                     You can add `pg: dg.ResourceParam[PostgresResource]` to your asset
                     parameters to get accees to the resource.
    data_type: upper case data type as provided by CAL-ACCESS, e.g. "DEBT", "S496", etc
    table_name: name of the landing table the data will be inserted into,
                e.g. "ca_cvr_f530"
    table_columns_name: list of columns name, be sure to have the table column be in
                        the same order as the file columns order as well.
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs

    """

    logger = dg.get_dagster_logger(name=f"ca_{data_type}_insert")

    data_type_file_path = get_ca_raw_data_file_by_data_type(data_type=data_type)

    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info("Truncating table before inserting the new one")
        pg_cursor.execute(query=truncate_query)

        try:
            data_type_lines_generator = safe_readline_csv_like_file(data_type_file_path)
            parsed_data_type_file = csv.reader(
                data_type_lines_generator,
                delimiter="\t",
                quotechar=None,
            )

            # Skip the headers
            next(parsed_data_type_file)

            insert_parsed_file_to_landing_table(
                pg_cursor=pg_cursor,
                csv_reader=parsed_data_type_file,
                table_name=table_name,
                table_columns_name=table_columns_name,
                row_validation_callback=data_validation_callback,
            )
        except FileNotFoundError:
            logger.warning(f"File for data {data_type} is non-existent, ignoring...")
        except Exception as e:
            logger.error(f"Got error while reading data type {data_type} file: {e}")
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
def ca_fetch_cal_access_raw_data(context: dg.AssetExecutionContext):
    """
    Fetch the latest CAL-ACCESS raw data, then unzip it to the CA data path prefix.

    As stated on CA Secretary of State, the data is updated daily.
    https://www.sos.ca.gov/campaign-lobbying/helpful-resources/raw-data-campaign-finance-and-lobbying-activity

    The only data that's going to be unzipped is on the "./DATA" path on the zip file.
    As the rest of the data is not really useful for campaign finance data.
    """

    Path(CA_DATA_PATH_PREFIX).mkdir(parents=True, exist_ok=True)

    context.log.info("Starting to download CAL-ACCESS raw data")

    temp_cal_access_zip_file_path = CA_DATA_PATH_PREFIX + "/dbexport.zip"
    stream_download_file_to_path(
        request_url=CA_CAL_ACCESS_RAW_DATA_URL,
        file_save_path=temp_cal_access_zip_file_path,
    )

    try:
        with ZipFile(temp_cal_access_zip_file_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                # Only extract the DATA part of the zip
                if file_info.filename.startswith(CA_CAL_ACCESS_DATA_PREFIX):
                    cleaned_file_name = file_info.filename.removeprefix(
                        CA_CAL_ACCESS_DATA_PREFIX
                    )
                    clean_file_extract_path = (
                        CA_DATA_PATH_PREFIX + "/" + cleaned_file_name
                    )

                    context.log.info(
                        f"Extracting CAL-ACCESS file to {clean_file_extract_path}"
                    )

                    if file_info.filename.endswith("/"):  # It's a directory
                        os.makedirs(clean_file_extract_path, exist_ok=True)
                        continue

                    os.makedirs(os.path.dirname(clean_file_extract_path), exist_ok=True)

                    # Extract file using buffered streaming
                    with (
                        zip_ref.open(file_info) as source,
                        open(clean_file_extract_path, "wb") as target,
                    ):
                        shutil.copyfileobj(source, target, 65536)  # 64KB buffer
    finally:
        if os.path.exists(temp_cal_access_zip_file_path):
            os.remove(temp_cal_access_zip_file_path)


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr_e530_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserting all the CVR E530 data to landing table.

    No description was given of what E530 is meant to be, but it's not
    CAL-ACCESS format. Though it is listed as campaign data on the Readme.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR_E530",
        table_name="ca_cvr_e530",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "rec_type",
            "form_type",
            "entity_cd",
            "filer_naml",
            "filer_namf",
            "filer_namt",
            "filer_nams",
            "report_num",
            "rpt_date",
            "filer_city",
            "filer_st",
            "filer_zip4",
            "occupation",
            "employer",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "district_cd",
            "office_cd",
            "pmnt_dt",
            "pmnt_amount",
            "type_literature",
            "type_printads",
            "type_radio",
            "type_tv",
            "type_it",
            "type_billboards",
            "type_other",
            "other_desc",
        ],
        data_validation_callback=lambda row: len(row) == 32,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_f501_502_insert_to_landing_table(pg: dg.ResourceParam[PostgresResource]):
    """
    Inserting all the F501 503 data to landing table.

    F501 is the Candidate Intention Statement.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="F501_503",
        table_name="ca_f501_502",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "rec_type",
            "form_type",
            "filer_id",
            "committee_id",
            "entity_cd",
            "report_num",
            "rpt_date",
            "stmt_type",
            "from_date",
            "thru_date",
            "elect_date",
            "cand_naml",
            "cand_namf",
            "can_namm",
            "cand_namt",
            "cand_nams",
            "moniker_pos",
            "moniker",
            "cand_city",
            "cand_st",
            "cand_zip4",
            "cand_phon",
            "cand_fax",
            "cand_email",
            "fin_naml",
            "fin_namf",
            "fin_namt",
            "fin_nams",
            "fin_city",
            "fin_st",
            "fin_zip4",
            "fin_phon",
            "fin_fax",
            "fin_email",
            "office_cd",
            "offic_dscr",
            "agency_nam",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "party",
            "yr_of_elec",
            "elec_type",
            "execute_dt",
            "can_sig",
            "account_no",
            "acct_op_dt",
            "party_cd",
            "district_cd",
            "accept_limit_yn",
            "did_exceed_dt",
            "cntrb_prsnl_fnds_dt",
        ],
        data_validation_callback=lambda row: len(row) == 54,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr3_verification_info_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR3 Verification Info data to landing table.

    This data is part of F401 form (Slate Mailer Organization Statement).
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR3_VERIFICATION_INFO",
        table_name="ca_cvr3_verification_info",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "sig_date",
            "sig_loc",
            "sig_naml",
            "sig_namf",
            "sig_namt",
            "sig_nams",
        ],
        data_validation_callback=lambda row: len(row) == 13,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr_so_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR SO data to landing table.

    No description was given for this data, but it was listed as the
    campaign information on the Readme Docs given by CAL-ACCESS.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR_SO",
        table_name="ca_cvr_so",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "rec_type",
            "form_type",
            "filer_id",
            "entity_cd",
            "filer_naml",
            "filer_namf",
            "filer_namt",
            "filer_nams",
            "report_num",
            "rpt_date",
            "qual_cb",
            "qualfy_dt",
            "term_date",
            "city",
            "st",
            "zip4",
            "phone",
            "county_res",
            "county_act",
            "mail_city",
            "mail_st",
            "mail_zip4",
            "cmte_fax",
            "cmte_email",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "tres_phon",
            "actvty_lvl",
            "com82013yn",
            "com82013nm",
            "com82013id",
            "control_cb",
            "bank_nam",
            "bank_adr1",
            "bank_adr2",
            "bank_city",
            "bank_st",
            "bank_zip4",
            "bank_phon",
            "acct_opendt",
            "surplusdsp",
            "primfc_cb",
            "genpurp_cb",
            "gpc_descr",
            "sponsor_cb",
            "brdbase_cb",
            "smcont_qualdt",
        ],
        data_validation_callback=lambda row: len(row) == 54,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr2_so_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR2 SO data to landing table.

    The data is additional names/commitees information for
    the forms 400 (Statement of Organization (Slate Mailer Organization)
    and 410 (Statement of Organization Recipient Committee).
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR2_SO",
        table_name="ca_cvr2_so",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "enty_naml",
            "enty_namf",
            "enty_namt",
            "enty_nams",
            "item_cd",
            "mail_city",
            "mail_st",
            "mail_zip4",
            "day_phone",
            "fax_phone",
            "email_adr",
            "cmte_id",
            "ind_group",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "non_pty_cb",
            "party_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
            "year_elect",
            "pof_title",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr_campaign_disclosure_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR Campaign Disclosure data to landing table.

    Cover page information for the campaign disclosure forms
    (F401, F450, F460, F461, F425, F465, F496, F497, F498).
    This data comes from the electronic filing. The data
    contained herein is "as filed" by the entity making the filing.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR_CAMPAIGN_DISCLOSURE",
        table_name="ca_cvr_campaign_disclosure",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "rec_type",
            "form_type",
            "filer_id",
            "entity_cd",
            "filer_naml",
            "filer_namf",
            "filer_namt",
            "filer_nams",
            "report_num",
            "rpt_date",
            "stmt_type",
            "late_rptno",
            "from_date",
            "thru_date",
            "elect_date",
            "filer_city",
            "filer_st",
            "filer_zip4",
            "filer_phon",
            "filer_fax",
            "file_email",
            "mail_city",
            "mail_st",
            "mail_zip4",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "tres_phon",
            "tres_fax",
            "tres_email",
            "cmtte_type",
            "control_yn",
            "sponsor_yn",
            "primfrm_yn",
            "brdbase_yn",
            "amendexp_1",
            "amendexp_2",
            "amendexp_3",
            "rpt_att_cb",
            "cmtte_id",
            "reportname",
            "rptfromdt",
            "rptthrudt",
            "emplbus_cb",
            "bus_name",
            "bus_city",
            "bus_st",
            "bus_zip4",
            "bus_inter",
            "busact_cb",
            "busactvity",
            "assoc_cb",
            "assoc_int",
            "other_cb",
            "other_int",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "cand_city",
            "cand_st",
            "cand_zip4",
            "cand_phon",
            "cand_fax",
            "cand_email",
            "bal_name",
            "bal_num",
            "bal_juris",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "sup_opp_cd",
            "employer",
            "occupation",
            "selfemp_cb",
            "bal_id",
            "cand_id",
        ],
        data_validation_callback=lambda row: len(row) == 86,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr2_campaign_disclosure_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR2 Campaign Disclosure data to landing table.

    Record used to carry additional names for the campaign
    disclosure forms 401, 450, 460, 461, 425, 465, 496, 497,
    and 498.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR2_CAMPAIGN_DISCLOSURE",
        table_name="ca_cvr2_campaign_disclosure",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "title",
            "mail_city",
            "mail_st",
            "mail_zip4",
            "f460_part",
            "cmte_id",
            "enty_naml",
            "enty_namf",
            "enty_namt",
            "enty_nams",
            "enty_city",
            "enty_st",
            "enty_zip4",
            "enty_phon",
            "enty_fax",
            "enty_email",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "control_yn",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
        ],
        data_validation_callback=lambda row: len(row) == 38,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_cvr_f470_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the CVR F470 data to landing table.

    Cover page layout for F470 Officeholder/Candidate Short
    Supplement
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="CVR_F470",
        table_name="ca_cvr_f470",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "rec_type",
            "form_type",
            "filer_id",
            "entity_cd",
            "filer_naml",
            "filer_namf",
            "filer_namt",
            "filer_nams",
            "report_num",
            "rpt_date",
            "cand_city",
            "cand_st",
            "cand_zip4",
            "cand_phon",
            "cand_fax",
            "cand_email",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "elect_date",
            "date_1000",
        ],
        data_validation_callback=lambda row: len(row) == 26,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_s401_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the S401 data to landing table.

    This table contains Form 401 payment and other
    disclosure schedule (F401B, F401B-1, F401C, F401D)
    information.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="S401",
        table_name="ca_s401",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "agent_naml",
            "agent_namf",
            "agent_namt",
            "agent_nams",
            "payee_naml",
            "payee_namf",
            "payee_namt",
            "payee_nams",
            "payee_city",
            "payee_st",
            "payee_zip4",
            "amount",
            "aggregate",
            "expn_dscr",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
            "memo_code",
            "memo_refno",
            "bakref_tid",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_f495p2_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the F495P2 data to landing table.

    This table contains Form 495 Part II contribution
    information.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="F495P2",
        table_name="ca_f495p2",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "elect_date",
            "electjuris",
            "contribamt",
        ],
        data_validation_callback=lambda row: len(row) == 8,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_s496_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the S496 data to landing table.

    This table contains Form 496 Late Independent
    Expenditures Made data.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="S496",
        table_name="ca_s496",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "amount",
            "exp_date",
            "expn_dscr",
            "memo_code",
            "memo_refno",
            "date_thru",
        ],
        data_validation_callback=lambda row: len(row) == 12,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_s497_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the S497 data to landing table.

    This table contains information for Form 497 Late
    Contributions Received/Made filings.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="S497",
        table_name="ca_s497",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "enty_naml",
            "enty_namf",
            "enty_namt",
            "enty_nams",
            "enty_city",
            "enty_st",
            "enty_zip4",
            "ctrib_emp",
            "ctrib_occ",
            "ctrib_self",
            "elec_date",
            "ctrib_date",
            "date_thru",
            "amount",
            "cmte_id",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "memo_code",
            "memo_refno",
            "bal_id",
            "cand_id",
            "sup_off_cd",
            "sup_opp_cd",
        ],
        data_validation_callback=lambda row: len(row) == 41,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_s498_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the S498 data to landing table.

    This table contains Form 498 Late Independent
    Expenditures Made information.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="S498",
        table_name="ca_s498",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "cmte_id",
            "payor_naml",
            "payor_namf",
            "payor_namt",
            "payor_nams",
            "payor_city",
            "payor_st",
            "payor_zip4",
            "date_rcvd",
            "amt_rcvd",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
            "amt_attrib",
            "memo_code",
            "memo_refno",
            "employer",
            "occupation",
            "selfemp_cb",
        ],
        data_validation_callback=lambda row: len(row) == 37,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_debt_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the DEBT data to landing table.

    Form 460 Schedule (F) Accrued Expenses (Unpaid Bills)
    records.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="DEBT",
        table_name="ca_debt",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "payee_naml",
            "payee_namf",
            "payee_namt",
            "payee_nams",
            "payee_city",
            "payee_st",
            "payee_zip4",
            "beg_bal",
            "amt_incur",
            "amt_paid",
            "end_bal",
            "expn_code",
            "expn_dscr",
            "cmte_id",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "memo_code",
            "memo_refno",
            "bakref_tid",
            "xref_schnm",
            "xref_match",
        ],
        data_validation_callback=lambda row: len(row) == 33,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_loan_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the LOAN data to landing table.

    Loan Schedules / Received (B1, B2, B3) and Made (H1, H2, H3)
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="LOAN",
        table_name="ca_loan",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "loan_type",
            "entity_cd",
            "lndr_naml",
            "lndr_namf",
            "lndr_namt",
            "lndr_nams",
            "loan_city",
            "loan_st",
            "loan_zip4",
            "loan_date1",
            "loan_date2",
            "loan_amt1",
            "loan_amt2",
            "loan_amt3",
            "loan_amt4",
            "loan_rate",
            "loan_emp",
            "loan_occ",
            "loan_self",
            "cmte_id",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "intr_naml",
            "intr_namf",
            "intr_namt",
            "intr_nams",
            "intr_city",
            "intr_st",
            "intr_zip4",
            "memo_code",
            "memo_refno",
            "bakref_tid",
            "xref_schnm",
            "xref_match",
            "loan_amt5",
            "loan_amt6",
            "loan_amt7",
            "loan_amt8",
        ],
        data_validation_callback=lambda row: len(row) == 49,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_expn_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the EXPN data to landing table.

    This table contains expenditure records for the Form 460
    Schedules D, E, and G, the Form450 Part 5, the Form 461
    Part 5, Form 465 Part 3)
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="EXPN",
        table_name="ca_expn",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "payee_naml",
            "payee_namf",
            "payee_namt",
            "payee_nams",
            "payee_city",
            "payee_st",
            "payee_zip4",
            "expn_date",
            "amount",
            "cum_ytd",
            "cum_oth",
            "expn_chkno",
            "expn_code",
            "expn_dscr",
            "agent_naml",
            "agent_namf",
            "agent_namt",
            "agent_nams",
            "cmte_id",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
            "memo_code",
            "memo_refno",
            "bakref_tid",
            "g_from_e_f",
            "xref_schnm",
            "xref_match",
        ],
        data_validation_callback=lambda row: len(row) == 53,
    )


@dg.asset(deps=[ca_fetch_cal_access_raw_data], pool="pg")
def ca_rcpt_insert_to_landing_table(
    pg: dg.ResourceParam[PostgresResource],
):
    """
    Inserting all the RCPT data to landing table.

    Receipts schedules for the Form 460 Schedules A, C, I,
    and A-1 and the Form 401 Schedule A.
    """

    return ca_insert_raw_file_to_landing_table(
        postgres_pool=pg.pool,
        data_type="RCPT",
        table_name="ca_rcpt",
        table_columns_name=[
            "filing_id",
            "amend_id",
            "line_item",
            "rec_type",
            "form_type",
            "tran_id",
            "entity_cd",
            "ctrib_naml",
            "ctrib_namf",
            "ctrib_namt",
            "ctrib_nams",
            "ctrib_city",
            "ctrib_st",
            "ctrib_zip4",
            "ctrib_emp",
            "ctrib_occ",
            "ctrib_self",
            "tran_type",
            "rcpt_date",
            "date_thru",
            "amount",
            "cum_ytd",
            "cum_oth",
            "ctrib_dscr",
            "cmte_id",
            "tres_naml",
            "tres_namf",
            "tres_namt",
            "tres_nams",
            "tres_city",
            "tres_st",
            "tres_zip4",
            "intr_naml",
            "intr_namf",
            "intr_namt",
            "intr_nams",
            "intr_city",
            "intr_st",
            "intr_zip4",
            "intr_emp",
            "intr_occ",
            "intr_self",
            "cand_naml",
            "cand_namf",
            "cand_namt",
            "cand_nams",
            "office_cd",
            "offic_dscr",
            "juris_cd",
            "juris_dscr",
            "dist_no",
            "off_s_h_cd",
            "bal_name",
            "bal_num",
            "bal_juris",
            "sup_opp_cd",
            "memo_code",
            "memo_refno",
            "bakref_tid",
            "xref_schnm",
            "xref_match",
            "int_rate",
            "intr_cmteid",
        ],
        data_validation_callback=lambda row: len(row) == 63,
    )

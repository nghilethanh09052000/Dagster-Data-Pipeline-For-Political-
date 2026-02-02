import csv
import sys
import time
import urllib.parse
from collections.abc import Callable
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

csv.field_size_limit(sys.maxsize)
DATA_PATH_PREFIX = "./states/washington"
DATASETS = [
    {
        "name": "campaign_finance_reporting_history",
        "api_name": "https://data.wa.gov/api/views/7qr9-q2c9/rows.csv",
        "columns": [
            "report_number",
            "amends_report",
            "amended_by_report",
            "origin",
            "committee_id",
            "filer_id",
            "election_year",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "ballot_number",
            "for_or_against",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "receipt_date",
            "filing_method",
            "report_from",
            "report_through",
            "url",
            "user_data",
            "metadata",
            "version",
            "attachments",
        ],
    },
    {
        "name": "campaign_finance_summary",
        "api_name": "https://data.wa.gov/api/views/3h9x-7bvm/rows.csv",
        "columns": [
            "id",
            "filer_id",
            "filer_type",
            "registered",
            "declared",
            "withdrew",
            "discontinued",
            "filing_type",
            "receipt_date",
            "election_year",
            "candidate_committee_status",
            "filer_name",
            "committee_acronym",
            "committee_address",
            "committee_city",
            "committee_county",
            "committee_state",
            "committee_zip",
            "committee_email",
            "candidate_email",
            "candidate_committee_phone",
            "office",
            "office_code",
            "jurisdiction",
            "jurisdiction_code",
            "jurisdiction_county",
            "jurisdiction_type",
            "jurisdiction_voters",
            "jurisdiction_reporting_code",
            "jurisdiction_reporting_requirement",
            "committee_category",
            "political_committee_type",
            "bonafide_committee",
            "bonafide_type",
            "position",
            "party_code",
            "party",
            "election_date",
            "reporting_option",
            "active_candidate",
            "on_primary_election_ballot",
            "on_general_election_ballot",
            "primary_election_status",
            "general_election_status",
            "election_status",
            "exempt_nonexempt",
            "ballot_committee",
            "ballot_proposal_count",
            "ballot_number",
            "ballot_title",
            "for_or_against",
            "ballot_proposal_detail",
            "other_pac",
            "treasurer_address",
            "treasurer_city",
            "treasurer_state",
            "treasurer_zip",
            "treasurer_phone",
            "contributions_amount",
            "carryforward_amount",
            "expenditures_amount",
            "loans_amount",
            "pledges_amount",
            "debts_amount",
            "independent_expenditures_for_amount",
            "independent_expenditures_against_amount",
            "url",
            "committee_id",
            "person_id",
            "candidacy_id",
            "fund_id",
            "legislative_district",
            "treasurer_name",
            "pac_type",
            "continuing",
            "updated_at",
        ],
    },
    {
        "name": "candidate_surplus_funds_latest_report",
        "api_name": "https://data.wa.gov/api/views/3cbn-54c3/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "filer_id",
            "person_id",
            "filer_name",
            "from_date",
            "thru_date",
            "year",
            "raised",
            "spent",
            "balance",
            "url",
        ],
    },
    {
        "name": "candidate_surplus_funds_reports",
        "api_name": "https://data.wa.gov/api/views/9kcu-2bem/rows.csv",
        "columns": [
            "id",
            "report_number",
            "amends_report",
            "amended_by_report",
            "origin",
            "filer_id",
            "person_id",
            "filer_name",
            "from_date",
            "thru_date",
            "year",
            "raised",
            "spent",
            "balance",
            "url",
            "committee_id",
            "metadata",
            "attachments",
            "version",
        ],
    },
    {
        "name": "contributions_to_candidates_and_political_committees",
        "api_name": "https://data.wa.gov/api/views/kv7h-kjye/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "committee_id",
            "filer_id",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "ballot_number",
            "for_or_against",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "election_year",
            "amount",
            "cash_or_in_kind",
            "receipt_date",
            "description",
            "memo",
            "primary_general",
            "code",
            "contributor_category",
            "contributor_name",
            "contributor_address",
            "contributor_city",
            "contributor_state",
            "contributor_zip",
            "contributor_occupation",
            "contributor_employer_name",
            "contributor_employer_city",
            "contributor_employer_state",
            "url",
            "contributor_location",
        ],
    },
    {
        "name": "lobbyist_employers_summary",
        "api_name": "https://data.wa.gov/api/views/biux-xiwe/rows.csv",
        "columns": [
            "id",
            "year",
            "employer_name",
            "employer_email",
            "employer_phone",
            "employer_address",
            "employer_city",
            "employer_state",
            "employer_zip",
            "employer_country",
            "compensation",
            "expenditures",
            "agg_contrib",
            "ballot_prop",
            "entertain",
            "vendor",
            "expert_retain",
            "inform_material",
            "lobbying_comm",
            "ie_in_support",
            "itemized_exp",
            "other_l3_exp",
            "political",
            "corr_compensation",
            "corr_expend",
            "total_exp",
            "l3_nid",
            "employer_nid",
            "url",
        ],
    },
    {
        "name": "lobbyist_summary",
        "api_name": "https://data.wa.gov/api/views/c4ag-3cmj/rows.csv",
        "columns": [
            "id",
            "year",
            "filer_id",
            "lobbyist_name",
            "lobbyist_email",
            "lobbyist_phone",
            "firm_address",
            "firm_city",
            "firm_state",
            "firm_zip",
            "firm_country",
            "temp_phone",
            "temp_address",
            "temp_city",
            "temp_state",
            "temp_zip",
            "temp_country",
            "compensation",
            "sub_lobbyist_compensation",
            "net_compensation",
            "personal_expenses",
            "entertainment",
            "contributions",
            "pac_contributions",
            "advertising",
            "political_ads",
            "independent_expenditures_candidate",
            "independent_expenditures_ballot",
            "other",
            "direct_lobbying_expenses",
            "indirect_lobbying_expenses",
            "total_expenses",
            "net_total",
            "use_categories",
            "periods",
        ],
    },
    {
        "name": "pledges_reporting_history",
        "api_name": "https://data.wa.gov/api/views/8bva-rkeb/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "committee_id",
            "filer_id",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "ballot_number",
            "for_or_against",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "election_year",
            "amount",
            "receipt_date",
            "primary_general",
            "code",
            "contributor_name",
            "contributor_address",
            "contributor_city",
            "contributor_state",
            "contributor_zip",
            "contributor_occupation",
            "contributor_employer_name",
            "contributor_employer_city",
            "contributor_employer_state",
            "url",
        ],
    },
    {
        "name": "pre_2016_lobbyist_compensation_and_expenses_by_source",
        "api_name": "https://data.wa.gov/api/views/3v2j-kqbi/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "filer_id",
            "filer_name",
            "type",
            "funding_source_id",
            "funding_source_name",
            "filing_period",
            "receipt_date",
            "employer_id",
            "employer_name",
            "compensation",
            "personal_expenses",
            "entertainment",
            "contributions",
            "advertising",
            "political_ads",
            "other",
            "new_filer_id",
            "new_employer_id",
        ],
    },
    {
        "name": "pre_2016_lobbyist_employment_registration",
        "api_name": "https://data.wa.gov/api/views/x2x6-7bd8/rows.csv",
        "columns": [
            "id",
            "lobbyist_id",
            "lobbyist_name",
            "employer_id",
            "employer_name",
            "employment_year",
            "contractor",
            "contractor_name",
            "new_filer_id",
            "new_employer_id",
        ],
    },
    {
        "name": "debt_reported_by_candidates_and_political_committees",
        "api_name": "https://data.wa.gov/api/views/3r6b-hsaa/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "committee_id",
            "filer_id",
            "filer_type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "election_year",
            "amount",
            "record_type",
            "from_date",
            "thru_date",
            "debt_date",
            "code",
            "description",
            "vendor_name",
            "vendor_address",
            "vendor_city",
            "vendor_state",
            "vendor_zip",
            "url",
        ],
    },
    {
        "name": "public_agency_lobbying_totals",
        "api_name": "https://data.wa.gov/api/views/mjwb-szba/rows.csv",
        "columns": [
            "id",
            "filer_id",
            "name",
            "address",
            "city",
            "state",
            "zip_code",
            "contact",
            "email_address",
            "phone",
            "year",
            "q1_report_num",
            "q1",
            "q2_report_num",
            "q2",
            "q3_report_num",
            "q3",
            "q4_report_num",
            "q4",
            "year_total",
            "q1_url",
            "q2_url",
            "q3_url",
            "q4_url",
        ],
    },
    {
        "name": "expenditures_by_candidates_and_political_committees",
        "api_name": "https://data.wa.gov/api/views/tijg-9zyp/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "committee_id",
            "filer_id",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "ballot_number",
            "for_or_against",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "election_year",
            "amount",
            "itemized_or_non_itemized",
            "expenditure_date",
            "description",
            "code",
            "recipient_name",
            "recipient_address",
            "recipient_city",
            "recipient_state",
            "recipient_zip",
            "url",
            "recipient_location",
            "payee",
            "creditor",
        ],
    },
    {
        "name": "surplus_funds_expenditures",
        "api_name": "https://data.wa.gov/api/views/ti55-mvy5/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "filer_id",
            "person_id",
            "type",
            "filer_name",
            "election_year",
            "amount",
            "itemized_or_non_itemized",
            "expenditure_date",
            "description",
            "code",
            "recipient_name",
            "recipient_address",
            "recipient_city",
            "recipient_state",
            "recipient_zip",
            "url",
            "recipient_location",
        ],
    },
    {
        "name": "financial_affairs_disclosures",
        "api_name": "https://data.wa.gov/api/views/ehbc-shxw/rows.csv",
        "columns": [
            "id",
            "submission_id",
            "report_number",
            "filer_id",
            "person_id",
            "name",
            "receipt_date",
            "period_start",
            "period_end",
            "json",
            "url",
            "certification_name",
            "certification_email",
            "certification_phone",
            "candidacies",
            "offices",
            "certification",
            "history",
        ],
    },
    {
        "name": "voter_address_precinct_crosswalk",
        "api_name": "https://data.wa.gov/api/views/37cr-k5cr/rows.csv",
        "columns": [
            "streetaddress",
            "street_city",
            "street",
            "regstnum",
            "regcity",
            "regstate",
            "regzipcode",
            "precinctcode",
            "levycode",
            "countycode",
        ],
    },
    {
        "name": "imaged_documents_and_reports",
        "api_name": "https://data.wa.gov/api/views/j78t-andi/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "document_description",
            "filer_id",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "party",
            "election_year",
            "receipt_date",
            "processed_date",
            "filing_method",
            "report_from",
            "report_to",
            "url",
        ],
    },
    {
        "name": "voter_precinct_to_jurisdiction_crosswalk",
        "api_name": "https://data.wa.gov/api/views/efcw-k4fa/rows.csv",
        "columns": [
            "districtid",
            "precinctid",
            "precinctcode",
            "levycode",
            "countycode",
            "jurisdiction_code",
            "office_code",
            "jurisdiction_county",
        ],
    },
    {
        "name": "independent_campaign_expenditures_and_electioneering_communications",
        "api_name": "https://data.wa.gov/api/views/67cp-h962/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "sponsor_entity_id",
            "sponsor_id",
            "sponsor_name",
            "sponsor_description",
            "report_type",
            "report_date",
            "election_year",
            "sponsor_address",
            "sponsor_city",
            "sponsor_state",
            "sponsor_zip",
            "sponsor_email",
            "sponsor_phone",
            "total_unitemized",
            "total_cycle",
            "total_this_report",
            "expenditure_amount",
            "expenditure_description",
            "date_expense_obligated",
            "date_advertising_presented",
            "vendor_name",
            "vendor_address",
            "vendor_city",
            "vendor_state",
            "vendor_zipcode",
            "candidate_entity_id",
            "candidate_candidacy_id",
            "candidate_committee_id",
            "candidate_filer_id",
            "candidate_name",
            "candidate_last_name",
            "candidate_first_name",
            "candidate_office",
            "candidate_jurisdiction",
            "candidate_party",
            "candidate_office_type",
            "ballot_name",
            "ballot_number",
            "ballot_type",
            "portion_of_amount",
            "for_or_against",
            "funders_name",
            "funders_first_name",
            "funders_middle_initial",
            "date_received",
            "amount",
            "funders_address",
            "funders_city",
            "funders_state",
            "funders_zipcode",
            "funders_occupation",
            "funders_employer",
            "funders_employer_city",
            "funders_employer_state",
            "url",
            "sponsor_location",
            "vendor_location",
            "funders_location",
            "filer_id",
        ],
    },
    {
        "name": "loans_to_candidates_and_political_committees",
        "api_name": "https://data.wa.gov/api/views/d2ig-r3q4/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "committee_id",
            "filer_id",
            "type",
            "filer_name",
            "office",
            "legislative_district",
            "position",
            "party",
            "jurisdiction",
            "jurisdiction_county",
            "jurisdiction_type",
            "election_year",
            "cash_or_in_kind",
            "receipt_date",
            "repayment_schedule",
            "loan_due_date",
            "lender_or_endorser",
            "transaction_type",
            "amount",
            "endorser_liable_amount",
            "primary_general",
            "lenders_name",
            "lenders_address",
            "lenders_city",
            "lenders_state",
            "lenders_zip",
            "lenders_occupation",
            "lenders_employer",
            "employers_city",
            "employers_state",
            "url",
            "carry_forward_loan",
            "description",
        ],
    },
    {
        "name": "lobbyist_agent_employers",
        "api_name": "https://data.wa.gov/api/views/e7sd-jbuy/rows.csv",
        "columns": [
            "id",
            "filer_id",
            "lobbyist_firm_name",
            "lobbyist_phone",
            "lobbyist_email",
            "lobbyist_address",
            "employment_registration_id",
            "employment_registration_title",
            "employer_id",
            "employer_title",
            "agent_name",
            "agent_bio",
            "agent_pic_url",
            "employment_year",
            "lobbyist_firm_url",
            "employment_registration_url",
            "employer_url",
            "training_certified",
        ],
    },
    {
        "name": "lobbyist_agents",
        "api_name": "https://data.wa.gov/api/views/bp5b-jrti/rows.csv",
        "columns": [
            "id",
            "filer_id",
            "lobbyist_firm_name",
            "lobbyist_phone",
            "lobbyist_email",
            "lobbyist_address",
            "employers",
            "agent_name",
            "agent_bio",
            "agent_pic_url",
            "employment_year",
            "lobbyist_firm_url",
            "training_certified",
        ],
    },
    {
        "name": "lobbyist_compensation_and_expenses_by_source",
        "api_name": "https://data.wa.gov/api/views/9nnw-c693/rows.csv",
        "columns": [
            "id",
            "report_number",
            "origin",
            "filer_id",
            "filer_name",
            "type",
            "funding_source_id",
            "funding_source",
            "filing_period",
            "first_filed_at",
            "receipt_date",
            "employer_id",
            "employer_name",
            "compensation",
            "sub_lobbyist_compensation",
            "net_compensation",
            "personal_expenses",
            "entertainment",
            "contributions",
            "advertising",
            "political_ads",
            "independent_expenditures_candidate",
            "independent_expenditures_ballot",
            "other",
            "direct_expenses",
            "indirect_expenses",
            "contributions_total",
            "total_expenses",
            "net_total",
            "employment_registration_id",
            "employment_type",
            "contractor_id",
            "contractor_name",
            "url",
        ],
    },
    {
        "name": "pdc_enforcement_case_attachments",
        "api_name": "https://data.wa.gov/api/views/ub89-7wbv/rows.csv",
        "columns": [
            "id",
            "case",
            "case_opened",
            "subject",
            "document_type",
            "description",
            "file_name",
            "url",
            "case_url",
        ],
    },
    {
        "name": "pdc_enforcement_cases",
        "api_name": "https://data.wa.gov/api/views/a4ma-dq6s/rows.csv",
        "columns": [
            "id",
            "case",
            "opened",
            "earliest_complaint",
            "closed",
            "complainant",
            "respondent",
            "subject",
            "areas_of_law",
            "status",
            "group_enforcement",
            "description",
            "url",
            "total_penalties",
            "total_suspended",
            "total_reinstated",
            "total_paid",
            "balance_due",
            "sent_to_collections",
            "attachments",
            "penalties",
            "updated_at",
        ],
    },
    {
        "name": "out_of_state_committee_expenditures",
        "api_name": "https://data.wa.gov/api/views/mzg4-pm9n/rows.csv",
        "columns": [
            "id",
            "report_id",
            "filer_name",
            "committee_id",
            "committee_address",
            "committee_city",
            "committee_state",
            "committee_zip",
            "entity_id",
            "election_year",
            "expense_date",
            "amount",
            "recipient_type",
            "recipient_committee_email",
            "recipient_committee_id",
            "recipient_candidacy_id",
            "recipient_name",
            "recipient_office",
            "recipient_jurisdiction",
            "recipient_party",
            "description",
            "purpose",
            "ballot_number",
            "for_or_against",
            "inkind",
            "vendor_name",
            "vendor_city",
            "vendor_state",
            "vendor_postcode",
            "vendor_address",
            "url",
            "committee_email",
            "inkind_description",
        ],
    },
    {
        "name": "contributions_to_out_of_state_political_committees",
        "api_name": "https://data.wa.gov/api/views/qdtg-6yir/rows.csv",
        "columns": [
            "id",
            "report_id",
            "filer_name",
            "election_year",
            "committee_id",
            "committee_address",
            "committee_city",
            "committee_state",
            "committee_zip",
            "committee_email",
            "entity_id",
            "contribution_date",
            "aggregate_total",
            "amount",
            "resident",
            "name",
            "address",
            "city",
            "state",
            "postcode",
            "employer",
            "employer_address",
            "employer_city",
            "employer_state",
            "url",
        ],
    },
    {
        "name": "lobbyist_reporting_history",
        "api_name": "https://data.wa.gov/api/views/nuwx-ay5h/rows.csv",
        "columns": [
            "origin",
            "id",
            "amends_report",
            "amended_by_report",
            "filer_name",
            "filer_type",
            "entity_id",
            "firm_id",
            "client_id",
            "year",
            "receipt_date",
            "filing_method",
            "report_from",
            "report_through",
            "url",
            "report_data",
            "version",
        ],
    },
    {
        "name": "public_disclosure_reporting_periods",
        "api_name": "https://data.wa.gov/api/views/uwe8-9un3/rows.csv",
        "columns": [
            "id",
            "report_type",
            "reporting_period_type",
            "start_date",
            "end_date",
            "due_date",
            "election_date",
        ],
    },
]


def generate_api_url(api_url: str, columns: list[str]):
    """
    Generates a formatted API URL for querying data.

    Parameters:
    - api_url (str): The base API URL with query parameters.
    - columns (list[str]): List of columns to include in the SELECT query.

    Returns:
    - str: The full API URL with the SELECT query and required parameters.
    """
    base_url = api_url.split("?")[0]
    fourfour = api_url.split("/")[-2]
    timestamp = int(time.time())
    date_str = time.strftime("%Y%m%d")

    # Format the column list into a SELECT query
    query = f"SELECT {', '.join(f'`{col}`' for col in columns)}"
    encoded_query = urllib.parse.quote(query)

    full_url = (
        f"{base_url}?query={encoded_query}&"
        f"fourfour={fourfour}&"
        f"read_from_nbe=true&"
        f"version=2.1&"
        f"cacheBust={timestamp}&"
        f"date={date_str}&"
        f"accessType=DOWNLOAD"
    )

    return full_url


def wa_fetch_bulk_data(
    dataset_name: str,
    api_url: str,
    columns: list[str],
):
    """
    Fetches bulk data from the given API URL and saves it as a CSV file.

    Parameters:
    - dataset_name (str): The name of the dataset.
    - api_url (str): The base API URL for data retrieval.
    - columns (list[str]): List of columns to include in the API query.

    Returns:
    - None: Saves the fetched data as a CSV file in the designated directory.
    """
    logger = dg.get_dagster_logger(name="wa_fetch_bulk_data")

    url = generate_api_url(api_url, columns)
    logger.info(f"WA Starting Fetch Bulk Data For {url}")

    dataset_dir = Path(DATA_PATH_PREFIX)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    csv_file_path = dataset_dir / f"{dataset_name}.csv"

    stream_download_file_to_path(url, csv_file_path)
    logger.info(f"Saved csv file to {csv_file_path}")


def insert_data_to_landing_table(
    postgres_pool: ConnectionPool,
    dataset_name: str,
    table_name: str,
    columns: list[str],
    data_validation_callback: Callable,
    data_file: str,
):
    """
    Insert raw wa-ACCESS data to landing table in Postgres.

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

    data_file: the file path of the actual data file on the washingont folder,
                        so that we will have corrent path and use
                        it to ingest to table
    """

    logger = dg.get_dagster_logger(name=f"wa_{dataset_name}_insert")
    truncate_query = get_sql_truncate_query(table_name=table_name)

    with (
        postgres_pool.connection() as pg_connection,
        pg_connection.cursor() as pg_cursor,
    ):
        logger.info(f"Truncating table {table_name} before inserting new data.")
        pg_cursor.execute(truncate_query)

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


def wa_create_fetch_raw_data_assets(dataset: dict):
    """
    Creates a Dagster asset that fetches raw data from the
    specified API and processes it into a structured format.

    This asset is responsible for fetching data
    from API based on the provided dataset configuration.

    The asset fetches the raw data using the dataset's
    API URL and columns, and it is typically followed by an insert
    operation into a landing table.

    Args:
        dataset (dict[str, Union[str, list[str]]]):
            - 'name' (str): The name of the dataset.
            - 'api_name' (str): The URL of the API to fetch data from.
            - 'columns' (list[str]): A list of column names to
                process the raw data into a structured format.

    Returns:
        function: A Dagster asset function that fetches raw data from the API.
        This function will fetch and process the data when executed.

    The fetch asset performs the following:
        - It fetches the raw data from the API specified by `dataset["api_name"]`.
        - It processes the data based on the provided columns,
            structuring the raw data into a usable format.

    Example:
        wa_create_fetch_raw_data_assets({
            "name": "example_dataset",
            "api_name": "https://example.com/api/data",
            "columns": ["column1", "column2", "column3"]
        })
    """

    @dg.asset(
        name=f"wa_{dataset.get('name')}_fetch_raw_data",
    )
    def fetch_data():
        return wa_fetch_bulk_data(
            dataset_name=dataset["name"],
            api_url=dataset["api_name"],
            columns=dataset["columns"],
        )

    return fetch_data


def wa_insert_to_landing_table(dataset: dict):
    """
    Creates a Dagster asset that inserts data into a landing table
    in the PostgreSQL database.

    This asset is dependent on a corresponding fetch asset
    (which retrieves the raw data).
    It uses the provided dataset's name and columns to determine
    the landing table and perform the insert.

    Args:
        dataset (dict[str, Union[str, list[str]]]):
        A dictionary containing the dataset configuration with the following keys:
            - 'name' (str): The name of the dataset.
            - 'columns' (list[str]): A list of column names for the dataset.

    Returns:
        function: A Dagster asset function that inserts data
                into a landing table. It accepts a `PostgresResource` as an argument,
                  which is used for the database connection and operations.

    The insert asset performs the following:
        - It constructs the table name by appending '_landing' to the dataset name.
        - It defines the data file path based on the `DATA_PATH_PREFIX`
                 and dataset name.
        - It uses a callback for data validation, ensuring each row matches the
                    expected column length.
        - It inserts the validated data into the PostgreSQL landing table via the
                    `insert_data_to_landing_table` function.

    Dependencies:
        - The asset depends on a fetch asset with the
        name `wa_{dataset_name}_fetch_raw_data`.

    Example:
        wa_insert_to_landing_table({
            "name": "example_dataset",
            "columns": ["column1", "column2", "column3"]
            ...
        })
    """
    fetch_asset_name = f"wa_{dataset.get('name')}_fetch_raw_data"

    @dg.asset(
        deps=[fetch_asset_name],
        name=f"wa_{dataset.get('name')}_insert_to_landing_table",
    )
    def insert_data(pg: dg.ResourceParam[PostgresResource]):
        dataset_name = dataset["name"]
        table_name = f"wa_{dataset_name}_landing"
        columns = dataset["columns"]
        data_file = f"{DATA_PATH_PREFIX}/{dataset_name}.csv"

        return insert_data_to_landing_table(
            postgres_pool=pg.pool,
            dataset_name=dataset_name,
            table_name=table_name,
            columns=columns,
            data_validation_callback=lambda row: len(row) == len(columns),
            data_file=data_file,
        )

    return insert_data


fetch_assets = [wa_create_fetch_raw_data_assets(dataset) for dataset in DATASETS]

insert_assets = [wa_insert_to_landing_table(dataset) for dataset in DATASETS]

assets = fetch_assets + insert_assets

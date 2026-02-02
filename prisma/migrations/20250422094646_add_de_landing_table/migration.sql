CREATE TABLE de_filed_reports_landing (
    filing_period text,
    filing_method text,
    cf_id text,
    committee_name text,
    committee_type text,
    original_report_filed_date text,
    latest_report_filed_date text,
    reporting_year text,
    office text
);

CREATE TABLE de_contributions_landing (
    contribution_date text,
    contributor_name text,
    contributor_address_line1 text,
    contributor_address_line2 text,
    contributor_city text,
    contributor_state text,
    contributor_zip text,
    contributor_type text,
    employer_name text,
    employer_occupation text,
    contribution_type text,
    contribution_amount text,
    cf_id text,
    receiving_committee text,
    filing_period text,
    office text,
    fixed_asset text
);


CREATE TABLE de_committees_landing (
    committee_type text,
    cf_id text,
    committee_name text,
    office text,
    committee_status text,
    registered_date text,
    amendment_date text,
    tressurer_name text,
    tressurer_address text,
    empty_column text
);

CREATE TABLE de_expenditures_landing (
    expenditure_date text,
    payee_name text,
    payee_address_line_1 text,
    payee_address_line_2 text,
    payee_city text,
    payee_state text,
    payee_zip text,
    payee_type text,
    amount_dollars text,
    cf_id text,
    committee_name text,
    expense_category text,
    expense_purpose text,
    expense_method text,
    filing_period text,
    fixed_asset text,
    empty_column text
);

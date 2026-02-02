CREATE TABLE az_allowed_initial_transactions_landing (
    transaction_type_id text,
    name_type text,
    payment_type text,
    resulting_transaction_description text,
    combo_reference text
);

CREATE TABLE az_allowed_offset_transactions_landing (
    transaction_type_id text,
    offsets_combo_reference text,
    resulting_transaction_description text
);

CREATE TABLE az_committees_landing (
    committee_id text PRIMARY KEY,
    committee_name text,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    committee_type text,
    organization_date text,
    termination_date text
);

CREATE TABLE az_counties_landing (
    county_id text PRIMARY KEY,
    county_name text
);

CREATE TABLE az_reporting_periods_landing (
    cycle text,
    report_name text,
    reporting_period_begin_date text,
    reporting_period_end_date text,
    filing_period_begin_date text,
    filing_period_end_date text,
    report_type text
);

CREATE TABLE az_transaction_types_landing (
    transaction_type_id text,
    transaction_type_name text,
    is_initial text
);

CREATE TABLE az_transaction_types_allowed_by_committee_landing (
    combo_reference text,
    committee_type text
);

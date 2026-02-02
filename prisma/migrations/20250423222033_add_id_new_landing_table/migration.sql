-- Create ID new contributions landing table
CREATE TABLE "id_new_contributions_landing" (
    filing_entity_id TEXT,
    filing_entity_name TEXT,
    campaign_name TEXT,
    registration_type TEXT,
    transaction_id TEXT,
    transaction_type TEXT,
    transaction_sub_type TEXT,
    contributor_type TEXT,
    contributor_last_name TEXT,
    contributor_first_name TEXT,
    contributor_address_line_1 TEXT,
    contributor_address_line_2 TEXT,
    contributor_address_city TEXT,
    contributor_address_state TEXT,
    contributor_address_zip_code TEXT,
    transaction_date TEXT,
    transaction_amount TEXT,
    loan_interest_amount TEXT,
    total_loan_amount TEXT,
    election_type TEXT,
    election_year TEXT,
    transaction_description TEXT,
    amended TEXT,
    timed_report_name TEXT,
    timed_report_date TEXT,
    report_name TEXT,
    report_filed_date TEXT
);

-- Make the table unlogged for faster bulk loading
ALTER TABLE "id_new_contributions_landing" SET UNLOGGED;

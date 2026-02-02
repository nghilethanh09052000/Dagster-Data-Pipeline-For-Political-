-- Create Louisiana Contributions Landing Table
CREATE TABLE la_contributions_landing (
    -- Filer Information
    filer_last_name TEXT,
    filer_first_name TEXT,
    -- Report Information
    report_code TEXT,
    report_type TEXT,
    report_number TEXT,
    -- Contributor Information
    contributor_type_code TEXT,
    contributor_name TEXT,
    contributor_addr1 TEXT,
    contributor_addr2 TEXT,
    contributor_city TEXT,
    contributor_state TEXT,
    contributor_zip TEXT,
    -- Contribution Information
    contribution_type TEXT,
    contribution_description TEXT,
    contribution_date TEXT,
    contribution_amt TEXT,
    contribution_designated_election_addition_info TEXT
);

-- Create Louisiana Expenditures Landing Table
CREATE TABLE la_expenditures_landing (
    -- Filer Information
    filer_last_name TEXT,
    filer_first_name TEXT,
    -- Report Information
    report_code TEXT,
    report_type TEXT,
    report_number TEXT,
    schedule TEXT,
    -- Recipient Information
    recipient_name TEXT,
    recipient_addr1 TEXT,
    recipient_addr2 TEXT,
    recipient_city TEXT,
    recipient_state TEXT,
    recipient_zip TEXT,
    -- Expenditure Information
    expenditure_description TEXT,
    candidate_beneficiary TEXT,
    expenditure_date TEXT,
    expenditure_amt TEXT
);

-- Create Louisiana Candidates Landing Table
CREATE TABLE la_candidates_landing (
    candidate_name TEXT,
    filer_type TEXT,
    election_date TEXT,
    office TEXT
);


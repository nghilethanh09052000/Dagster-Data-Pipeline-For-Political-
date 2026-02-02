-- Create landing table for New Mexico Candidate transactions
CREATE TABLE nm_transactions_candidate_landing (
    first_name TEXT,
    last_name TEXT,
    description TEXT,
    is_contribution TEXT,
    is_anonymous TEXT,
    amount TEXT,
    date_contribution TEXT,
    memo TEXT,
    contrib_expenditure_description TEXT,
    contrib_expenditure_first_name TEXT,
    contrib_expenditure_middle_name TEXT,
    contrib_expenditure_last_name TEXT,
    suffix TEXT,
    company_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    occupation TEXT,
    filing_period TEXT,
    date_added TEXT
);


-- Create landing table for New Mexico PAC transactions
CREATE TABLE nm_transactions_pac_landing (
    pac_name TEXT,
    description TEXT,
    is_contribution TEXT,
    is_anonymous TEXT,
    amount TEXT,
    date_contribution TEXT,
    memo TEXT,
    contrib_expenditure_description TEXT,
    contrib_expenditure_first_name TEXT,
    contrib_expenditure_middle_name TEXT,
    contrib_expenditure_last_name TEXT,
    suffix TEXT,
    company_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    occupation TEXT,
    filing_period TEXT,
    date_added TEXT
);


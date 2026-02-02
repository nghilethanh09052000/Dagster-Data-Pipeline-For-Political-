CREATE TABLE ks_contribution_landing (
    candidate_name TEXT,
    contributor_name TEXT,
    contributor_address TEXT,
    contributor_city TEXT,
    contributor_state TEXT,
    contributor_zip TEXT,
    occupation TEXT,
    industry TEXT,
    date_received TEXT,
    tender_type TEXT,
    amount TEXT,
    in_kind_amount TEXT,
    in_kind_description TEXT,
    period_start TEXT,
    period_end TEXT
);


CREATE TABLE ks_expenditure_landing (
    candidate_name TEXT,
    recipient TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    date TEXT,
    expenditure_description TEXT,
    amount TEXT,
    start_date TEXT,
    end_date TEXT
);


CREATE TABLE ks_candidate_landing (
    candidate TEXT,
    office TEXT,
    party TEXT,
    title TEXT,
    first_name TEXT,
    middle TEXT,
    last_name TEXT,
    suffix TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    mailing_address TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    home_phone TEXT,
    work_phone TEXT,
    cell_phone TEXT,
    email TEXT,
    web_address TEXT,
    date_filed TEXT,
    ballot_city TEXT
);

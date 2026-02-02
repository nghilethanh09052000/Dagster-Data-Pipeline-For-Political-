


CREATE TABLE tn_candidates_landing (
    salutation TEXT,
    first_name TEXT,
    last_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    phone TEXT,
    email TEXT,
    treasurer_name TEXT,
    treasurer_address_1 TEXT,
    treasurer_address_2 TEXT,
    treasurer_city TEXT,
    treasurer_state TEXT,
    treasurer_zip TEXT,
    treasurer_phone TEXT,
    treasurer_email TEXT,
    party_affiliation TEXT
);

CREATE TABLE tn_contributions_landing (
    type TEXT,
    adj TEXT,
    amount TEXT,
    date TEXT,
    election_year TEXT,
    report_name TEXT,
    recipient_name TEXT,
    contributor_name TEXT,
    contributor_address TEXT,
    contributor_occupation TEXT,
    contributor_employer TEXT,
    description TEXT
);

CREATE TABLE tn_expenditures_landing (
    type TEXT,
    adj TEXT,
    amount TEXT,
    "date" TEXT,
    election_year TEXT,
    report_name TEXT,
    candidate_pac_name TEXT,
    vendor_name TEXT,
    vendor_address TEXT,
    purpose TEXT,
    candidate_for TEXT,
    s_o TEXT
);

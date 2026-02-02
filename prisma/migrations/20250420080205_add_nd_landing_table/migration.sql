CREATE TABLE nd_candidates_landing (
    candidate_name text,
    committee_name text,
    street text,
    city text,
    state text,
    zip text,
    type text,
    office text,
    district text
);

CREATE TABLE nd_committees_landing (
    committee_name text,
    street text,
    city text,
    state text,
    zip text,
    phone text,
    email text,
    committee_type text
);

CREATE TABLE nd_contributions_landing (
    contributor text,
    street text,
    city text,
    state text,
    zip text,
    date text,
    amount text,
    contributed_to text,
    empty_column text
); 

CREATE TABLE nd_expenditures_landing (
    expenditure_made_by text,
    street text,
    city text,
    state text,
    zip text,
    date text,
    amount text,
    recipient text
); 
DROP TABLE nd_candidates_landing;
DROP TABLE nd_committees_landing;
DROP TABLE nd_expenditures_landing;



CREATE TABLE nd_candidates_landing (
    candidate_name text,
    committee_name text,
    street_city text,
    state text,
    zip text,
    type text,
    office text,
    district text,
    year text
);

CREATE TABLE nd_committees_landing (
    year text,
    committee_name text,
    street text,
    city text,
    state text,
    zip text,
    phone text,
    email text,
    committee_type text,
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
    recipient text,
    empty_column text
); 
-- See MASTER_TABLES_README.md for more information

-- Individual Contributions Master Table
CREATE TABLE master_individual_contributions (
    id bigint PRIMARY KEY,
    source text NOT NULL,
    source_id text,
    committee_id text NOT NULL,
    name text NOT NULL,
    city text,
    state text,
    zip_code text NOT NULL,
    employer text,
    occupation text,
    amount decimal NOT NULL,
    contribution_datetime timestamptz NOT NULL,
    insert_datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Committee Master Table
CREATE TABLE master_committee (
    committee_id text PRIMARY KEY,
    source text NOT NULL,
    committee_designation text,
    name text NOT NULL,
    address1 text NOT NULL,
    address2 text NOT NULL,
    city text NOT NULL,
    state text NOT NULL,
    zip_code text NOT NULL,
    affiliation text,
    district text,
    insert_datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

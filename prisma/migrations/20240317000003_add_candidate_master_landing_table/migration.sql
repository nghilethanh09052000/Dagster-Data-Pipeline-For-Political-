-- Based on https://www.fec.gov/campaign-finance-data/candidate-master-file-description/
CREATE TABLE federal_candidate_master_landing (
    cand_id text PRIMARY KEY,
    cand_name text,
    cand_pty_affiliation text,
    cand_election_yr text,
    cand_office_st text,
    cand_office text,
    cand_office_district text,
    cand_ici text,
    cand_status text,
    cand_pcc text,
    cand_st1 text,
    cand_st2 text,
    cand_city text,
    cand_st text,
    cand_zip text
);

-- Based on https://www.fec.gov/campaign-finance-data/committee-master-file-description/
CREATE TABLE federal_committee_master_landing (
    cmte_id text PRIMARY KEY,
    cmte_nm text,
    tres_nm text,
    cmte_st1 text,
    cmte_st2 text,
    cmte_city text,
    cmte_st text,
    cmte_zip text,
    cmte_dsgn text,
    cmte_tp text,
    cmte_pty_affiliation text,
    cmte_filing_freq text,
    org_tp text,
    connected_org_nm text,
    cand_id text
);

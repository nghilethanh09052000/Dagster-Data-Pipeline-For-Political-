-- Based on https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/
CREATE TABLE federal_individual_contributions_landing (
    cmte_id text,
    amndt_ind text,
    rpt_tp text,
    transaction_pgi text,
    image_num text,
    transaction_tp text,
    entity_tp text,
    name text,
    city text,
    state text,
    zip_code text,
    employer text,
    occupation text,
    transaction_dt text,
    transaction_amt text,
    other_id text,
    tran_id text,
    file_num text,
    memo_cd text,
    memo_text text,
    sub_id text PRIMARY KEY
);

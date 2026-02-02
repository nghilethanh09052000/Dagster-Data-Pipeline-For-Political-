
DROP TABLE IF EXISTS "id_contributions_landing";

CREATE TABLE "id_old_contributions_landing" (
    cand_last text NULL,
    cand_suf text NULL,
    cand_first text NULL,
    cand_mi text NULL,
    party text NULL,
    office text NULL,
    district text NULL,
    date text NULL,
    amount text NULL,
    type text NULL,
    contr_last text NULL,
    contr_first text NULL,
    address text NULL,
    city text NULL,
    state text NULL,
    zip text NULL,
    purpose text NULL
);

-- Make the table unlogged for faster bulk loading
ALTER TABLE "id_old_contributions_landing" SET UNLOGGED;

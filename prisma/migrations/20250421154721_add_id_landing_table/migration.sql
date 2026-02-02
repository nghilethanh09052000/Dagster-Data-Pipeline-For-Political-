-- Create ID contributions landing table
CREATE TABLE "id_contributions_landing" (
    "party" TEXT,
    "cand_first" TEXT,
    "cand_mi" TEXT,
    "cand_last" TEXT,
    "cand_suffix" TEXT,
    "committee" TEXT,
    "office" TEXT,
    "district" TEXT,
    "type" TEXT,
    "amount" TEXT,
    "date" TEXT,
    "last" TEXT,
    "first" TEXT,
    "mi" TEXT,
    "suffix" TEXT,
    "address_1" TEXT,
    "address_2" TEXT,
    "city" TEXT,
    "state" TEXT,
    "zip" TEXT,
    "country" TEXT,
    "election" TEXT,
    "source_file" TEXT,
    "amend" TEXT,
    "con_type" TEXT,
    "source_type" TEXT
);

-- Make the table unlogged for faster bulk loading
ALTER TABLE "id_contributions_landing" SET UNLOGGED;

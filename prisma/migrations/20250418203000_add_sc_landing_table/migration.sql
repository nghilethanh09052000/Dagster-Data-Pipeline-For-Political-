-- Create South Carolina contributions landing table
CREATE TABLE "sc_contributions_landing" (
    "contribution_id" TEXT,
    "office_run_id" TEXT,
    "candidate_id" TEXT,
    "date" TEXT,
    "amount" TEXT,
    "candidate_name" TEXT,
    "office_name" TEXT,
    "election_date" TEXT,
    "contributor_name" TEXT,
    "contributor_occupation" TEXT,
    "group" TEXT,
    "contributor_address" TEXT,
    "description" TEXT
);

-- Make the table unlogged for faster bulk loading
ALTER TABLE "sc_contributions_landing" SET UNLOGGED;
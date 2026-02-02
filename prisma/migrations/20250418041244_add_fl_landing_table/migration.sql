-- Create Florida contributions landing table
CREATE TABLE IF NOT EXISTS "fl_contributions_landing" (
    "Candidate/Committee" TEXT,
    "Date" TEXT,
    "Amount" TEXT,
    "Type" TEXT,
    "Contributor_Name" TEXT,
    "Address" TEXT,
    "City_State_Zip" TEXT,
    "Occupation" TEXT,
    "InKind_Desc" TEXT
);
DROP TABLE md_contributions_landing;
DROP TABLE md_expenditures_landing;

CREATE TABLE md_contributions_and_loans_landing (
    "ReceivingCommittee" text,
    "FilingPeriod" text,
    "ContributionDate" text,
    "ContributorName" text,
    "ContributorAddress" text,
    "ContributorType" text,
    "ContributionType" text,
    "ContributionAmount" text,
    "EmployerName" text,
    "EmployerOccupation" text,
    "Office" text,
    "Fundtype" text,
    "Unused" text
);

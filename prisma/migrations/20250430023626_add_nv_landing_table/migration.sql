CREATE TABLE nv_candidates_landing (
    "CandidateID" text,
    "FirstName" text,
    "LastName" text,
    "Party" text,
    "Office" text,
    "Jurisdiction" text,
    "MailingAddress" text,
    "MailingCity" text,
    "MailingState" text,
    "MailingZip" text
);

CREATE TABLE nv_groups_landing (
    "GroupID" text,
    "GroupName" text,
    "GroupType" text,
    "ContactName" text,
    "Active" text,
    "City" text
);

CREATE TABLE nv_reports_landing (
    "ReportID" text,
    "CandidateID" text,
    "GroupID" text,
    "ReportName" text,
    "ElectionCycle" text,
    "FilingDueDate" text,
    "FiledDate" text,
    "Amended" text,
    "Superseded" text
);

CREATE TABLE nv_contributors_payees_landing (
    "ContactID" text,
    "FirstName" text,
    "MiddleName" text,
    "LastName" text,
    "Address1" text,
    "Address2" text,
    "City" text,
    "State" text,
    "Zip" text
);

CREATE TABLE nv_contributions_landing (
    "ContributionID" text,
    "ReportID" text,
    "CandidateID" text,
    "GroupID" text,
    "ContributionDate" text,
    "ContributionAmount" text,
    "ContributionType" text,
    "ContributorID" text
);

CREATE TABLE nv_expenses_landing (
    "ExpenseID" text,
    "ReportID" text,
    "CandidateID" text,
    "GroupID" text,
    "ExpenseDate" text,
    "ExpenseAmount" text,
    "ExpenseType" text,
    "PayeeID" text
);

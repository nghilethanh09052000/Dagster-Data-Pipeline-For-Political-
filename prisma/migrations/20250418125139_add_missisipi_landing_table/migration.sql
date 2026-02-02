CREATE TABLE ms_contributions_landing (
    "Recipient" text,
    "ReferenceNumber" text,
    "FilingDesc" text,
    "FilingId" text,
    "Contributor" text,
    "ContributorType" text,
    "AddressLine1" text,
    "City" text,
    "StateCode" text,
    "PostalCode" text,
    "InKind" text,
    "Occupation" text,
    "Date" text,
    "Amount" text
);

CREATE TABLE ms_expenditures_landing (
    "Filer" text,
    "ReferenceNumber" text,
    "FilingDesc" text,
    "FilingId" text,
    "Recipient" text,
    "AddressLine1" text,
    "City" text,
    "StateCode" text,
    "PostalCode" text,
    "Description" text,
    "Date" text,
    "Amount" text
);

CREATE TABLE ms_candidate_committee_landing (
    "EntityId" text,
    "EntityName" text,
    "OrganizationType" text
);

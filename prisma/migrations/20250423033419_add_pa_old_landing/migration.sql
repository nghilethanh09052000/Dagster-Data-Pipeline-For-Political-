CREATE TABLE pennsylvania_old_contrib_landing (
    "FilerIdentificationNumber" text,
    "Year" text,
    "ReportCycleCode" text,
    "SectionCode" text,
    "ContributorName" text,
    "ContributorAddress1" text,
    "ContributorAddress2" text,
    "ContributorCity" text,
    "ContributorState" text,
    "ContributorZipCode" text,
    "ContributorOccupation" text,
    "EmployerName" text,
    "EmployerAddress1" text,
    "EmployerAddress2" text,
    "EmployerCity" text,
    "EmployerState" text,
    "EmployerZipCode" text,
    "ContributionDate1" text,
    "ContributionAmount1" text,
    "ContributionDate2" text,
    "ContributionAmount2" text,
    "ContributionDate3" text,
    "ContributionAmount3" text,
    "ContributionDescription" text
);

CREATE TABLE pennsylvania_old_debt_landing (
    "FilerIdentificationNumber" text,
    "Year" text,
    "ReportCycleCode" text,
    "CreditorName" text,
    "CreditorAddress1" text,
    "CreditorAddress2" text,
    "CreditorCity" text,
    "CreditorState" text,
    "CreditorZipCode" text,
    "DateDebtIncurred" text,
    "AmountofDebt" text,
    "DescriptionofDebt" text
);

CREATE TABLE pennsylvania_old_expense_landing (
    "FilerIdentificationNumber" text,
    "Year" text,
    "ReportCycleCode" text,
    "RecipientName" text,
    "RecipientAddress1" text,
    "RecipientAddress2" text,
    "RecipientCity" text,
    "RecipientState" text,
    "RecipientZipCode" text,
    "ExpenditureDate" text,
    "ExpenditureAmount" text,
    "ExpenditureDescription" text
);

CREATE TABLE pennsylvania_old_filer_landing (
    "FilerIdentificationNumber" text,
    "Year" text,
    "ReportTypeOrCycleCode" text,
    "Amendment" text,
    "Termination" text,
    "FilerTypeCode" text,
    "FilerName" text,
    "FilerOffice" text,
    "FilerDistrict" text,
    "FilerParty" text,
    "FilerAddress1" text,
    "FilerAddress2" text,
    "FilerCity" text,
    "FilerState" text,
    "FilerZipCode" text,
    "FilerCounty" text,
    "FilerPhone" text,
    "BeginningCashBalance" text,
    "UnitemizedMonetaryContributions" text,
    "UnitemizedInKindContributions" text
);

CREATE TABLE pennsylvania_old_receipt_landing (
    "FilerIdentificationNumber" text,
    "Year" text,
    "ReportCycleCode" text,
    "SourceofReceiptName" text,
    "SourceofReceiptAddress1" text,
    "SourceofReceiptAddress2" text,
    "SourceofReceiptCity" text,
    "SourceofReceiptState" text,
    "SourceofReceiptZipCode" text,
    "ReceiptDescription" text,
    "ReceiptDate" text,
    "ReceiptAmount" text
);

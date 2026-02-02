CREATE TABLE mo_cd1_a_landing_table (
    "CD1_A ID" text, 
    "MECID" text,
    "Committee Name" text,
    "Committee" text,
    "Company" text,
    "First Name" text,
    "Last Name" text,
    "Address 1" text,
    "Address 2" text,
    "City" text,
    "State" text,
    "Zip" text,
    "Employer" text,
    "Occupation" text,
    "Date" text,
    "Amount" text,
    "Contribution Type" text,
    "Report" text
);

CREATE TABLE mo_cd1_b_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Total Anonymous" text,
    "Total In-Kind" text,
    "Total Monetary" text
);

CREATE TABLE mo_cd1_c_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Lender Name" text,
    "Lender Address1" text,
    "Lender Address2" text,
    "Lender City" text,
    "Lender State" text,
    "Lender Zip" text,
    "Date Received" text,
    "Amount" text
);

CREATE TABLE mo_cd1a_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Event Location" text,
    "Location Address1" text,
    "Location Address2" text,
    "Location City" text,
    "Location State" text,
    "Location Zip" text,
    "Event Description" text,
    "Event Date" text,
    "Number of Participants" text,
    "Total Anonymous" text,
    "Anonymous Explaination" text
);

CREATE TABLE mo_cd1b1_a_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Loan RecordID" text,
    "Lender Name" text,
    "Lender Address1" text,
    "Lender Address2" text,
    "Lender City" text,
    "Lender State" text,
    "Lender Zip" text,
    "Date Received" text,
    "Loan Amount" text,
    "Loan ID" text,
    "Interest Rate" text,
    "Laon Period" text,
    "Loan Repayment Schedule" text,
    "FName Person Liable For Loan" text,
    "LName Person Liable For Loan" text,
    "Address1 Person Liable For Loan" text,
    "Address2 Person Liable For Loan" text,
    "City Person Liable For Loan" text,
    "State Person Liable For Loan" text,
    "Zip Person Liable For Loan" text
);

CREATE TABLE mo_cd1b2_a_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Lender" text,
    "Date" text,
    "Amount" text,
    "Type" text,
    "Payment Method" text
);

CREATE TABLE mo_cd3_a_landing_table (
    "MECID" text,
    "Report Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Expenditure Category" text,
    "Amount" text,
    "Expenditure Type" text,
    "Payment Method" text
);

CREATE TABLE mo_cd3_b_landing_table (
    "CD3_B ID" text, 
    "MECID" text,
    "Committee Name" text,
    "First Name" text,
    "Last Name" text,
    "Company" text,
    "Address 1" text,
    "Address 2" text,
    "City" text,
    "State" text,
    "Zip" text,
    "Date" text,
    "Purpose" text,
    "Amount" text,
    "Expenditure Type" text,
    "Report" text
);

CREATE TABLE mo_cd3_c_landing_table (
    "CD3_C ID" text, 
    "MECID" text,
    "Committee Name" text,
    "Committee" text,
    "Address 1" text,
    "Address 2" text,
    "City" text,
    "State" text,
    "Zip" text,
    "Date" text,
    "Amount" text,
    "Contribution Type" text,
    "Report" text
);

CREATE TABLE mo_committee_data_landing_table (
    "MECID" text,
    "Committee Type" text,
    "Committee Name" text,
    "Committee Status" text,
    "Acitve Name" text
);

CREATE TABLE mo_summary_landing_table (
    "MECID" text,
    "Committee Name" text,
    "ReportID" text,
    "CoverPageID" text,
    "Report" text,
    "Report Year" text,
    "Report Type" text,
    "Previous Receipts" text,
    "Contributions Received" text,
    "Loans Received" text,
    "Misc. Receipts" text, 
    "Receipts Subtotal" text,
    "In-Kind Contributions Received" text,
    "Total Receipts This Election" text,
    "Previous Expenditures" text,
    "Cash or Check Expenditures" text,
    "In-Kind Expenditures" text,
    "Credit Expenditures" text,
    "Expenditure Subtotal" text,
    "Total Expenditures" text,
    "Previous Contributions" text,
    "Cash/Check Contributions" text,
    "Credit Contributions" text,
    "In-Kind Contributions Made" text,
    "Contribution Subtotal" text,
    "Total Contributions" text,
    "Loan Disbursements" text,
    "Disbursements Payments" text,
    "Misc. Disbursements" text,
    "Total Disbursements" text,
    "Starting Money on Hand" text,
    "Monetary Receipts" text,
    "Check Disbursements" text,
    "Cash Disbursements" text,
    "Total Monetary Disbursements" text,
    "Ending Money on Hand" text,
    "Outstanding Indebtedness" text,
    "Loans Recieved" text,
    "New Expenditures" text,
    "New Contributions" text,
    "Payments Made on Loan" text,
    "Debt Forgiven on Loans" text,
    "Total Indebtendness" text
);
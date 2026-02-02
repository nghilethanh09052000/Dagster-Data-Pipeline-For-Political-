
CREATE TABLE il_candidates_landing (
  "LastName" text,
  "FirstName" text,
  "Address1" text,
  "Address2" text,
  "City" text,
  "State" text,
  "Zip" text,
  "Office" text,
  "DistrictType" text,
  "District" text,
  "ResidenceCounty" text,
  "PartyAffiliation" text,
  "RedactionRequested" text
);

CREATE TABLE il_cmte_candidate_links_landing (
  "CommitteeID" text,
  "CandidateID" text
);

CREATE TABLE il_cte_officer_links_landing (
  "CommitteeID" text,
  "OfficerID" text
);

CREATE TABLE il_committees_landing (
  "TypeOfCommittee" text,
  "StateCommittee" text,
  "StateID"	text,
  "LocalCommittee" text,
  "LocalID"	text,
  "ReferName"	text,
  "Name" text,
  "Address1" text,
  "Address2" text,
  "Address3" text,
  "City" text,
  "State" text,
  "Zip" text,
  "Status" text,
  "StatusDate" text,
  "CreationDate" text,
  "CreationAmount" text,
  "DispFundsReturn" text,
  "DispFundsPolComm" text,
  "DispFundsCharity" text,
  "DispFunds95" text,
  "DispFundsDescrip" text,
  "CanSuppOpp" text,
  "PolicySuppOpp" text,
  "PartyAffiliation" text,
  "Purpose" text
);

CREATE TABLE il_d2_totals_landing (
  "CommitteeID" text,
  "FiledDocID" text,
  "BegFundsAvail" text,
  "IndivContribI" text,
  "IndivContribNI" text,
  "XferInI" text,
  "XferInNI" text,
  "LoanRcvI" text,
  "LoanRcvNI" text,
  "OtherRctI"	text,
  "OtherRctNI" text,
  "TotalReceipts" text,
  "InKindI" text,
  "InKindNI" text,
  "TotalInKind" text,
  "XferOutI" text,
  "XferOutNI" text,
  "LoanMadeI" text,
  "LoanMadeNI" text,
  "ExpendI" text,
  "ExpendNI" text,
  "IndependentExpI" text,
  "IndependentExpNI" text,
  "TotalExpend" text,
  "DebtsI" text,
  "DebtsNI" text,
  "TotalDebts" text,
  "TotalInvest" text,
  "EndFundsAvail" text,
  "Archived" text
);

CREATE TABLE il_expenditures_landing (
  "CommitteeID" text,
  "FiledDocID" text,
  "ETransID" text,
  "LastOnlyName" text,
  "FirstName" text,
  "ExpendedDate" text,
  "Amount" text,
  "AggregateAmount" text,
  "Address1" text,
  "Address2" text,
  "City" text,
  "State" text,
  "Zip" text,
  "D2Part" text,
  "Purpose" text,
  "CandidateName" text,
  "Office" text,
  "Supporting" text,
  "Opposing" text,
  "Archived" text,
  "Country" text,			
  "RedactionRequested" text
);


CREATE TABLE il_investments_landing (
  "CommtteeID" text,
  "FiledDocID" text,
  "Description" text,
  "PurchaseDate" text,
  "PurchaseShares" text,
  "PurchasePrice" text,
  "CurrentValue" text,
  "LiquidValue" text,
  "LiquidDate" text,
  "LastOnlyName" text,
  "FirstName" text,
  "Address1" text,
  "Address2" text,
  "City" text,
  "State" text,
  "Zip" text,
  "Archived" text,
  "Country" text
);

CREATE TABLE il_officers_landing (
  "LastName" text,
  "FirstName" text,
  "Address1" text,
  "Address2" text,
  "City" text,
  "State" text,
  "Zip" text,
  "Title" text,
  "Phone" text,
  "RedactionRequested" text
);

CREATE TABLE il_receipts_landing (
  "CommitteeID" text,
  "FiledDocID" text,
  "ETransID" text,
  "LastOnlyName" text,
  "FirstName" text,
  "RcvDate" text,
  "Amount" text,
  "AggregateAmount" text,
  "LoanAmount" text,
  "Occupation" text,
  "Employer" text,
  "Address1" text,
  "Addresss2" text,
  "City" text,
  "State" text,
  "Zip" text,
  "D2Part" text,
  "Description" text,
  "VendorLastOnlyName" text,
  "VendorFirstName" text,
  "VendorAddress1" text,
  "VendorAddress2" text,
  "VendorCity" text,
  "VendorState" text,
  "VendorZip" text,
  "Archived" text,
  "Country" text,
  "RedactionRequested" text
);
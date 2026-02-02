ALTER TABLE il_candidates_landing ADD COLUMN "ID" text;

ALTER TABLE il_cmte_candidate_links_landing ADD COLUMN "ID" text;

ALTER TABLE il_cte_officer_links_landing ADD COLUMN "ID" text;

ALTER TABLE il_committees_landing ADD COLUMN "ID" text;

ALTER TABLE il_d2_totals_landing ADD COLUMN "ID" text;

ALTER TABLE il_expenditures_landing ADD COLUMN "ID" text;

ALTER TABLE il_investments_landing ADD COLUMN "ID" text,
DROP COLUMN "CommtteeID",
ADD COLUMN "CommitteeID" text;

ALTER TABLE il_officers_landing ADD COLUMN "ID" text;

ALTER TABLE il_receipts_landing ADD COLUMN "ID" text,
DROP COLUMN "Addresss2",
ADD COLUMN "Address2" text;

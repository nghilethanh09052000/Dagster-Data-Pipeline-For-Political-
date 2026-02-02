ALTER TABLE nh_new_expenditures_landing
ADD COLUMN "PartitionKey" TEXT;

ALTER TABLE nh_new_receipts_landing
ADD COLUMN "PartitionKey" TEXT;

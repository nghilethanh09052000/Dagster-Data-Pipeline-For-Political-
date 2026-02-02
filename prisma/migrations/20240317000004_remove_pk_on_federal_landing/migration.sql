ALTER TABLE federal_individual_contributions_landing
   DROP CONSTRAINT IF EXISTS federal_individual_contributions_landing_pkey;

ALTER TABLE federal_committee_master_landing
   DROP CONSTRAINT IF EXISTS federal_committee_master_landing_pkey;

ALTER TABLE federal_candidate_master_landing
   DROP CONSTRAINT IF EXISTS federal_candidate_master_landing_pkey;

-- Drop individual contributions, as we going to
-- truncate it and use raw schedule a data instead
drop table "federal_individual_contributions_landing";

-- schedule a data from FEC raw data
create table "federal_schedule_a_landing" (
    -- metadata to make our lives better
    partition_date text,

    -- v1
    form_type text,
    filer_committee_id_number text,
    -- Removed in v6.1
    contributor_name text,
    contributor_street_1 text,
    contributor_street_2 text,
    contributor_city text,
    contributor_state text,
    contributor_zip_code text,
    election_code text,
    election_other_description text,
    contributor_employer text,
    contributor_occupation text,
    contribution_aggregate text,
    contribution_date text,
    contribution_amount text,
    -- Removed in v8.0
    contribution_purpose_code text,
    contribution_purpose_descrip text,
    conduit_name text,
    conduit_street1 text,
    conduit_street2 text,
    conduit_city text,
    conduit_state text,
    conduit_zip_code text,
    donor_committee_fec_id text,
    donor_candidate_fec_id text,
    -- Removed in v6.1
    donor_candidate_name text,
    donor_candidate_office text,
    donor_candidate_state text,
    donor_candidate_district text,
    memo_code text,
    memo_text_description text,
    -- Removed in v6.1
    amended_cd text,

    -- v2
    entity_type text,
    transaction_id text,
    back_reference_tran_id_number text,
    back_reference_sched_name text,

    -- v3
    reference_code text,

    -- v5
    -- Removed from P2.6
    increased_limit_code text,

    -- v5.1
    contributor_organization_name text,
    -- No explanation the difference with contributor_name
    -- on the FEC docs
    contributor_last_name text,
    contributor_first_name text,
    contributor_middle_name text,
    contributor_prefix text,
    contributor_suffix text,

    -- v5.2 == v5.1 == v5.3

    -- v6.1
    donor_candidate_last_name text,
    donor_candidate_first_name text,
    donor_candidate_middle_name text,
    donor_candidate_prefix text,
    donor_candidate_suffix text,

    -- v6.2 - v6.3
    donor_committee_name text,

    -- No significant changes on v6.4 - v7.0

    -- No significant changes on v8.0 - v8.5,
    -- only removed contribution_purpose_code

    -- P1 - P2.4 (presidential, Form 3P)
    image_number text

    -- No significant change on P2.6 - P3.1,
    -- only removing increased_limit_code

    -- P3.2 - P3.4 just added memo_code
);

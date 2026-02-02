DROP TABLE id_old_contributions_landing;

CREATE TABLE id_old_candidate_contributions_landing (
    -- Candidate Information
    cand_last TEXT,
    cand_first TEXT,
    cand_middle TEXT,
    cand_suffix TEXT,
    party TEXT,
    office TEXT,
    district TEXT,

    -- Contribution Information
    contrib_date TEXT,
    contrib_amount TEXT,
    contrib_type TEXT,
    election_type TEXT,
    election_year TEXT,
    contrib_cp TEXT,

    -- Contributor Information
    contrib_committee_company TEXT,
    contrib_last TEXT,
    contrib_first TEXT,
    contrib_middle TEXT,
    contrib_suffix TEXT,
    contrib_line1 TEXT,
    contrib_line2 TEXT,
    contrib_city TEXT,
    contrib_state TEXT,
    contrib_zip TEXT,
    contrib_country TEXT
);

CREATE TABLE id_old_committee_contributions_landing (
    -- Committee Information
    committee_name TEXT,
    party TEXT,

    -- Contribution Information
    contrib_date TEXT,
    contrib_amount TEXT,
    contrib_type TEXT,
    election_type TEXT,
    election_year TEXT,
    contrib_cp TEXT,

    -- Contributor Information
    contributor_name TEXT,
    contrib_last TEXT,
    contrib_first TEXT,
    contrib_middle TEXT,
    contrib_suffix TEXT,
    contrib_line1 TEXT,
    contrib_line2 TEXT,
    contrib_city TEXT,
    contrib_state TEXT,
    contrib_zip TEXT,
    contrib_country TEXT
);

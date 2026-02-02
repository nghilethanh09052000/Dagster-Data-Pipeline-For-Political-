DROP TABLE al_campaign_committee;
DROP TABLE al_political_action_committee;

CREATE TABLE al_candidates_landing (
    candidate_name TEXT,
    party TEXT,
    office TEXT,
    district TEXT,
    place TEXT,
    committee_status TEXT,
    committee_id TEXT,
    address TEXT,
    committee_type TEXT,
    phone TEXT,
    date_registered TEXT,
    jurisdiction TEXT
);

CREATE TABLE al_political_action_committee_landing (
    committee_name TEXT,
    city TEXT,
    date_registered TEXT,
    committee_status TEXT,
    committee_id TEXT,
    address TEXT,
    committee_type TEXT,
    phone TEXT,
    jurisdiction TEXT
);

CREATE INDEX idx_al_candidates_status ON al_candidates_landing (committee_status);

CREATE INDEX idx_al_pac_committee_status ON al_political_action_committee_landing (committee_status);

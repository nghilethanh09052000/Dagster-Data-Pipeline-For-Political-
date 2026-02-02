-- Ground truth contributions table for benchmark evaluation
-- This table stores hand-researched contributions that serve as ground truth
-- for measuring data pipeline accuracy

CREATE TABLE IF NOT EXISTS ground_truth_contributions_manually_gathered_sample (
    state TEXT NOT NULL,
    fname TEXT NOT NULL,
    lname TEXT NOT NULL,
    zip TEXT NOT NULL,
    name_concat TEXT GENERATED ALWAYS AS (
        UPPER(fname || '|' || lname || '|' || LEFT(COALESCE(zip, ''), 5))
    ) STORED,
    amount NUMERIC,
    date DATE,
    source_url TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);


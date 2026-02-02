CREATE TABLE sync_lock_state (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    is_locked BOOLEAN DEFAULT FALSE,
    locked_at TIMESTAMP,
    locked_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE app_db_delta_rows (
    source TEXT,
    committee_id TEXT,
    source_id TEXT,
    name_concat TEXT,
    employer VARCHAR,
    occupation VARCHAR,
    amount INTEGER,
    contribution_datetime DATE,
    committee_name TEXT,
    candidate_id TEXT,
    candidate_name TEXT,
    candidate_office TEXT,
    insert_time TIMESTAMP DEFAULT NOW()
);

CREATE TABLE app_db_synced_rows (
    source_id VARCHAR(255) PRIMARY KEY,
    insert_time TIMESTAMP DEFAULT NOW(),
    synced_at TIMESTAMP DEFAULT NOW()
);

-- Create index for performance
CREATE INDEX idx_synced_rows_insert_time
ON app_db_synced_rows (insert_time);

-- Landing table for app database (should be created in app DB, not pipeline DB)

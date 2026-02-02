-- Helper script to populate benchmark_people_state_mapping table in app DB
-- Run this AFTER importing ground truth contacts into synthetic org
-- 
-- This creates a mapping of name_concat -> state for the imported people
-- so the Inngest job can report match rates by state

-- Replace 'YOUR_SYNTHETIC_ORG_ID' with actual organization_id
-- Replace 'YOUR_PIPELINE_DB_CONNECTION' with pipeline DB connection details

-- Option 1: If you have access to both DBs, you can run this directly:
-- (This assumes you can query pipeline DB from app DB, or you export/import)

-- Option 2: Export from pipeline DB and import into app DB
-- Export query (run in pipeline DB):
/*
SELECT DISTINCT
    name_concat,
    state
FROM ground_truth_contributions_manually_gathered_sample
ORDER BY state, name_concat;
*/

-- Then import into app DB mapping table:
/*
INSERT INTO benchmark_people_state_mapping (name_concat, state, organization_id)
SELECT 
    name_concat,
    state,
    'YOUR_SYNTHETIC_ORG_ID'  -- Replace with actual org ID
FROM (VALUES
    -- Paste exported data here, or load from CSV
    ('SMITH|JOHN|90210', 'CA'),
    ('DOE|JANE|90211', 'CA'),
    ...
) AS imported_data(name_concat, state)
ON CONFLICT (name_concat, organization_id) DO UPDATE SET
    state = EXCLUDED.state;
*/

-- Option 3: If you have cross-database access, use dblink or similar:
-- (Implementation depends on your setup)

-- Verify mapping:
/*
SELECT 
    state,
    COUNT(*) as count
FROM benchmark_people_state_mapping
WHERE organization_id = 'YOUR_SYNTHETIC_ORG_ID'
GROUP BY state
ORDER BY state;
*/



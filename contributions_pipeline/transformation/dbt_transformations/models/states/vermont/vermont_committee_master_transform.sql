{{ 
    config(
        materialized = 'table',
        tags = ["vermont", "committee", "master"]
    ) 
}}

-- ==========================================
-- Vermont Committee Master Transformation
-- ==========================================
-- Combines committee info from contributions,
-- expenditures, and committee reference data,
-- with optional candidate linkage.
-- ==========================================

WITH union_data AS (
    SELECT
        "FilingEntityId" AS committee_id,
        "CommitteeName" AS name
    FROM {{ source('vt', 'vermont_contributions_landing') }}
    WHERE "FilingEntityId" IS NOT NULL 
      AND "FilingEntityId" <> ''

    UNION

    SELECT
        "FilingEntityId" AS committee_id,
        "CommitteeName" AS name
    FROM {{ source('vt', 'vermont_expenditures_landing') }}
    WHERE "FilingEntityId" IS NOT NULL 
      AND "FilingEntityId" <> ''
),

union_grouped AS (
    SELECT
        committee_id,
        MAX(name) AS name
    FROM union_data
    GROUP BY committee_id
),

committee_data AS (
    SELECT 
        filing_entity_id AS committee_id,
        committee_name AS name,
        CASE committee_type
            WHEN 'Legislative Leadership PAC' THEN 'D'
            WHEN 'PAC' THEN 'P'
            WHEN 'Independent Expenditure Only PAC' THEN 'U'
            ELSE 'U'
        END AS committee_designation,
        committee_address AS address1,
        REGEXP_REPLACE(
            committee_address, 
            '.*([0-9]{5}(?:-[0-9]{4})?)$', 
            '\1'
        ) AS zip_code
    FROM {{ source('vt', 'vermont_committees_landing') }}
    WHERE filing_entity_id IS NOT NULL 
      AND filing_entity_id <> ''
      AND filing_entity_id <> 'Filing Entity Id'
),

grouped_committees AS (
    SELECT
        committee_id,
        MAX(name) AS name,
        MAX(committee_designation) AS committee_designation,
        MAX(address1) AS address1,
        MAX(zip_code) AS zip_code,
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS affiliation,
        NULL AS district
    FROM committee_data
    GROUP BY committee_id
),

merged AS (
    SELECT
        COALESCE(gc.committee_id, ug.committee_id) AS committee_id,
        CASE 
            WHEN ca.filing_entity_id IS NOT NULL THEN 'A' 
            ELSE COALESCE(gc.committee_designation, 'U')
        END AS committee_designation,
        COALESCE(gc.name, ug.name) AS name,
        'VT_' || ca.filing_entity_id AS candidate_id,
        CONCAT_WS(' ', ca.candidate_first_name, ca.candidate_middle_name, ca.candidate_last_name) AS candidate_name,
        gc.address1,
        gc.address2,
        gc.city,
        gc.state,
        gc.zip_code,
        gc.affiliation,
        gc.district
    FROM grouped_committees gc
    FULL JOIN union_grouped ug 
        ON gc.committee_id = ug.committee_id
    LEFT JOIN {{ source('vt', 'vermont_candidates_landing') }} ca
        ON ca.filing_entity_id = ug.committee_id
),

final AS (
    SELECT 
        'VT' AS source,
        'VT_' || committee_id AS committee_id,
        MAX(committee_designation) AS committee_designation,
        CASE 
            WHEN COALESCE(MAX(name), '') <> '' THEN MAX(name)
            ELSE MAX(candidate_name)
        END AS name,
        MAX(candidate_id) AS candidate_id,
        MAX(candidate_name) AS candidate_name,
        MAX(address1) AS address1,
        MAX(address2) AS address2,
        MAX(city) AS city,
        MAX(state) AS state,
        MAX(zip_code) AS zip_code,
        MAX(affiliation) AS affiliation,
        MAX(district) AS district
    FROM merged
    GROUP BY committee_id
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS insert_datetime
FROM final
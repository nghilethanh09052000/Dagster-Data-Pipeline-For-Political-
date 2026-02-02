{{
    config(
        materialized='table',
        tags=["new_mexico", "committee", "master"]
    )
}}

WITH source AS (
    SELECT
        org_id,
        committee_name,
        contributor_address_line_1 AS committee_address,
        contributor_city,
        contributor_state,
        contributor_zip_code,
        candidate_first_name,
        candidate_last_name,
        candidate_middle_name,
        candidate_prefix,
        candidate_suffix
    FROM {{ source('nm', 'nm_contributions_landing') }}
    WHERE committee_name IS NOT NULL 
      AND TRIM(committee_name) <> ''
      AND org_id IS NOT NULL
      AND TRIM(org_id) <> ''
)

, committees_with_candidates AS (
    -- Extract committees with candidate associations and committee data
    SELECT DISTINCT
        'NM' AS source,
        'NM_' || org_id AS committee_id,
        CASE 
            WHEN candidate_first_name IS NOT NULL 
                 AND candidate_last_name IS NOT NULL 
                 AND TRIM(candidate_first_name) != '' 
                 AND TRIM(candidate_last_name) != ''
            THEN 'NM_' || org_id
            ELSE NULL
        END AS candidate_id,
        CASE 
            WHEN candidate_first_name IS NOT NULL 
                 AND candidate_last_name IS NOT NULL 
                 AND TRIM(candidate_first_name) != '' 
                 AND TRIM(candidate_last_name) != ''
            THEN TRIM(CONCAT_WS(' ',
                NULLIF(candidate_first_name, ''),
                NULLIF(candidate_middle_name, ''),
                NULLIF(candidate_last_name, '')
            ))
            ELSE NULL
        END AS candidate_name,
        NULL AS committee_designation,
        committee_name AS name,
        committee_address AS address1,
        NULL AS address2,
        contributor_city AS city,
        contributor_state AS state,
        contributor_zip_code AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source
)

, ranked_committees AS (
    SELECT 
        *,
        row_number() OVER (
            PARTITION BY committee_id
        ) AS rn
    FROM committees_with_candidates
)

SELECT 
    source,
    committee_id,
    candidate_id,
    candidate_name,
    committee_designation,
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM ranked_committees
WHERE rn = 1
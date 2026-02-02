{{
    config(
      materialized='table',
      tags=["new_mexico", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        org_id,
        candidate_first_name,
        candidate_last_name,
        candidate_middle_name,
        candidate_prefix,
        candidate_suffix,
        election,
        committee_name
    FROM {{ source('nm', 'nm_contributions_landing') }}
    WHERE 
        candidate_first_name IS NOT NULL 
        AND candidate_last_name IS NOT NULL
        AND TRIM(candidate_first_name) <> ''
        AND TRIM(candidate_last_name) <> ''
        AND org_id IS NOT NULL
        AND TRIM(org_id) <> ''
)
, transformed_data AS (
    SELECT
        'NM' AS source,
        -- Generate candidate_id using org_id (always non-null due to filtering)
        'NM_' || org_id AS candidate_id,
        -- Concatenate candidate name components
        TRIM(CONCAT_WS(' ', 
            NULLIF(TRIM(candidate_prefix), ''),
            NULLIF(TRIM(candidate_first_name), ''),
            NULLIF(TRIM(candidate_middle_name), ''),
            NULLIF(TRIM(candidate_last_name), ''),
            NULLIF(TRIM(candidate_suffix), '')
        )) AS name,
        NULL AS affiliation,
        NULL AS district,
        election,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

, final as (
    SELECT 
        source,
        candidate_id,
        row_number() OVER (PARTITION BY candidate_id ORDER BY election DESC) AS row_num,
        name,
        affiliation,
        district,
        insert_datetime
    FROM transformed_data
)
select 
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
from final
where row_num = 1
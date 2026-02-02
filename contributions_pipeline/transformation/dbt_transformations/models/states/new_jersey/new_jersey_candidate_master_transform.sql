{{
    config(
      materialized='table',
      tags=["new_jersey", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        entity_s,
        entity_name,
        party
    FROM {{ source('nj', 'nj_candidates_landing') }}
    WHERE 
        entity_name IS NOT NULL 
        AND TRIM(entity_name) <> ''
        AND entity_s IS NOT NULL
    GROUP BY
        entity_s,
        entity_name,
        party
)
, transformed_data AS (
    SELECT
        'NJ' AS source,
        'NJ_' || entity_s AS candidate_id,
        INITCAP(TRIM(entity_name)) AS name,
        COALESCE(party, NULL) AS affiliation,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    insert_datetime
FROM transformed_data

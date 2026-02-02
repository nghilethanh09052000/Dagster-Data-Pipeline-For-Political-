{{
    config(
      materialized='table',
      tags=["alabama", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        candidate_name,
        party,
        office,
        district,
        committee_id
    FROM {{ source('al', 'al_candidates_landing') }}
    WHERE 
        candidate_name IS NOT NULL 
        AND TRIM(candidate_name) <> ''
        AND committee_id IS NOT NULL
)
, transformed_data AS (
    SELECT
        'AL' AS source,
        'AL_' || committee_id AS candidate_id,
        INITCAP(TRIM(candidate_name)) AS name,
        COALESCE(party, NULL) AS affiliation,
        COALESCE(district, NULL) AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT DISTINCT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

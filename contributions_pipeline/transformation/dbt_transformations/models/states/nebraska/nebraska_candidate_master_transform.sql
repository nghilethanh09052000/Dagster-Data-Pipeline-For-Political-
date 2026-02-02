{{
    config(
      materialized='table',
      tags=["nebraska", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        org_id,
        NULLIF(TRIM(candidate_name), '') AS candidate_name
    FROM {{ source('ne', 'ne_contributions_loan_landing') }}
    WHERE org_id IS NOT NULL
      AND TRIM(org_id) <> ''
      AND filer_type = 'Candidate Committee'
      AND candidate_name IS NOT NULL
      AND TRIM(candidate_name) <> ''
),

transformed AS (
    SELECT
        'NE' AS source,
        'NE_' || org_id AS candidate_id,
        candidate_name AS name,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
),

ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY candidate_id) AS rn
    FROM transformed
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM ranked
WHERE rn = 1



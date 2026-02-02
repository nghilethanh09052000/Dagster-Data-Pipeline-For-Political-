{{
    config(
        materialized='table',
        tags=["colorado", "candidate", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "CO_ID",
        "CandidateName",
        ROW_NUMBER() OVER (PARTITION BY "CO_ID") AS rn
    FROM {{ source('co', 'co_contributions_landing') }}
    WHERE "CO_ID" <> ''
      AND TRIM("CandidateName") <> '' 
)
SELECT
    'CO' AS source,
    'CO_' || "CO_ID" AS candidate_id,
    NULLIF("CandidateName", '') AS name,
    NULL AS affiliation,
    NULL AS district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE rn = 1
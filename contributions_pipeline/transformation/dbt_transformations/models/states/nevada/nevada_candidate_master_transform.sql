{{
    config(
      materialized='table',
      tags=["nevada", "candidate", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'NV' AS "source",
        "CandidateID",
        'NV_' || "CandidateID" AS candidate_id,
        COALESCE(NULLIF("FirstName", '') || ' ', '') || "LastName" AS "name",
        "Party" AS affiliation,
        ROW_NUMBER() OVER (
            PARTITION BY "CandidateID"
        ) AS row_num
    FROM {{ source('nv', 'nv_candidates_landing') }}
)

SELECT
    source,
    candidate_id,
    "CandidateID",
    "name",
    affiliation,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE row_num = 1

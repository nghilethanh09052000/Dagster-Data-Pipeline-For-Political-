{{
    config(
      materialized='table',
      tags=["ohio", "candidate_master"]
    )
}}

WITH source AS (
    SELECT
        TRIM(
            COALESCE("CANDIDATE_FIRST_NAME")
            || ' '
            || COALESCE("CANDIDATE_LAST_NAME")
        ) AS candidate_name,
        "PARTY" AS candidate_party
    FROM {{ source('oh', 'oh_candidate_list_landing') }}

    UNION ALL

    SELECT DISTINCT
        TRIM(
            COALESCE("CANDIDATE_FIRST_NAME")
            || ' '
            || COALESCE("CANDIDATE_LAST_NAME")
        ) AS candidate_name,
        "PARTY" AS candidate_party
    FROM {{ source('oh', 'oh_candidate_contribution_landing') }}

),

transformed AS (
    SELECT
        'OH' AS "source",
        'OH_'
        || {{ dbt_utils.generate_surrogate_key([
              'candidate_name',
              'candidate_party'
            ]) }} AS candidate_id,
        candidate_name AS "name",
        candidate_party AS affiliation,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source
),

numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY candidate_id) AS row_num
    FROM transformed
)

SELECT *
FROM numbered
WHERE row_num = 1

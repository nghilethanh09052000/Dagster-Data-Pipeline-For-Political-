{{ 
  config(
    materialized='table',
    tags=["alaska", "candidates", "master"]
  ) 
}}

WITH source_data AS (
    SELECT
        result,
        candidate,
        party,
        year,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('ak', 'ak_all_candidates_landing') }}
    WHERE candidate IS NOT NULL AND TRIM(candidate) <> ''
)
, transformed_data AS (
    SELECT
        'AK' AS source,
        'AK_' || {{ dbt_utils.generate_surrogate_key([
            'result',
            'candidate', 
            'party',
            'year'
        ]) }} AS candidate_id,
        candidate AS name,
        party AS affiliation,
        NULL AS district,
        insert_datetime
    FROM source_data
)
, ranked_data AS (
    SELECT
        source,
        candidate_id,
        name,
        affiliation,
        district,
        insert_datetime,
        ROW_NUMBER() OVER (PARTITION BY candidate_id) AS row_num
    FROM transformed_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM ranked_data
WHERE row_num = 1

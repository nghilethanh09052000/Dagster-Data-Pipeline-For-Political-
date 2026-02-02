{{ 
  config(
    materialized='table',
    tags=["delaware", "candidates", "master"]
  ) 
}}

WITH source_data AS (
    SELECT
        name,
        address,
        party,
        date_filed,
        phone,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('de', 'de_candidates_landing') }}
    WHERE name IS NOT NULL AND TRIM(name) <> ''
)
, transformed_data AS (
    SELECT
        'DE' AS source,
        'DE_' || {{ dbt_utils.generate_surrogate_key([
            'name',
            'party',
            'date_filed'
        ]) }} AS candidate_id,
        name AS name,
        party AS affiliation,
        insert_datetime
    FROM source_data
)
, ranked_data AS (
    SELECT
        source,
        candidate_id,
        name,
        affiliation,
        insert_datetime,
        ROW_NUMBER() OVER (PARTITION BY candidate_id) AS row_num
    FROM transformed_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    insert_datetime
FROM ranked_data
WHERE row_num = 1 
{{
    config(
      materialized='table',
      tags=["arkansas", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        guid,
        first_name,
        last_name,
        office,
        office_district_name,
        political_party
    FROM {{ source('ar', 'ar_candidates_landing') }}
    WHERE
        first_name IS NOT NULL AND TRIM(first_name) <> ''
        AND last_name IS NOT NULL AND TRIM(last_name) <> ''
),

transformed_data AS (
    SELECT
        'AR' AS source,
        'AR_' || guid AS candidate_id,
        INITCAP(NULLIF(first_name, '') || ' ' || last_name) AS name,
        political_party AS affiliation,
        office_district_name AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

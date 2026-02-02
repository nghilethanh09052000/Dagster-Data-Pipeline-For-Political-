{{
    config(
        materialized='table',
        tags=["louisiana", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT DISTINCT candidate_name
    FROM {{ source('la', 'la_candidates_landing') }}
),

transformed_data AS (
    SELECT
        'LA' AS "source",
        {{ generate_committee_id('LA_', 'candidate_name') }} AS committee_id,
        NULL AS committee_designation,
        candidate_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    "source",
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

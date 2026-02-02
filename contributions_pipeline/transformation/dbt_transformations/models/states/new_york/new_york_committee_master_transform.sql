{{
    config(
        materialized='table',
        tags=["new_york", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "FILER_ID" AS filer_id,
        "FILER_NAME" AS filer_name,
        "DISTRICT" AS district
    FROM {{ source('ny', 'ny_filer_data_landing') }}
),

cleaned_data AS (
    SELECT
        'NY' AS "source",
        'NY_' || filer_id AS committee_id,
        NULL AS committee_designation,
        filer_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
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
FROM cleaned_data

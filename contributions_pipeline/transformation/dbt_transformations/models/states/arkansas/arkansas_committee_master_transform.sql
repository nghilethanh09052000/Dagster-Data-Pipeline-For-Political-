{{
    config(
        materialized='table',
        tags=["arkansas", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'AR' AS source,
        'AR_' || "guid" AS committee_id,
        NULL AS committee_designation,
        "filerName" AS name,
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS zip_code,
        "politicalParty" AS affiliation,
        "officeDistrictName" AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('ar', 'ar_commitees_landing') }}
)

SELECT * FROM source_data


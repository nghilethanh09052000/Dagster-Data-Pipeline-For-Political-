{{
    config(
        materialized='table',
        tags=["mississippi", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "EntityId",
        "EntityName"
    FROM {{ source('ms', 'ms_candidate_committee_landing') }}
),

cleaned_data AS (
    SELECT
        'MS' AS "source",
        'MS_' || "EntityId" AS committee_id,
        NULL AS committee_designation,
        "EntityName" AS "name",
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
    source,
    committee_id,
    committee_designation,
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM cleaned_data


{{ config(
    materialized='table',
    tags=["oregon", "committee", "master"]
) }}

WITH source_data AS (
    SELECT
        "CommitteeId",
        "CommitteeName",
        "CandidateMalingAddress"
    FROM {{ source('or', 'or_committees_landing') }}
),

parsed_address AS (
    SELECT
        *,
        -- Full regex match: (address1 + city), state, zip
        (regexp_matches("CandidateMalingAddress", '^(.*)\s+([A-Z]{2})\s+(\d{5})'))[1] AS address_city_raw,
        (regexp_matches("CandidateMalingAddress", '^(.*)\s+([A-Z]{2})\s+(\d{5})'))[2] AS state,
        (regexp_matches("CandidateMalingAddress", '^(.*)\s+([A-Z]{2})\s+(\d{5})'))[3] AS zip_code,

        -- City = last word from address_city_raw
        reverse(split_part(reverse((regexp_matches("CandidateMalingAddress", '^(.*)\s+[A-Z]{2}\s+\d{5}$'))[1]), ' ', 1)) AS city,
        
        -- Address1 = what's left after removing city
        trim(
            regexp_replace(
                (regexp_matches("CandidateMalingAddress", '^(.*)\s+[A-Z]{2}\s+\d{5}$'))[1],
                '\s+' || reverse(split_part(reverse((regexp_matches("CandidateMalingAddress", '^(.*)\s+[A-Z]{2}\s+\d{5}$'))[1]), ' ', 1)) || '$',
                ''
            )
        ) AS address1
    FROM source_data
)

SELECT DISTINCT
    'OR' AS source,
    'OR_' || "CommitteeId" AS committee_id,
    NULL AS committee_designation,
    "CommitteeName" AS name,
    address1,
    NULL AS address2,
    NULLIF(TRIM(city), '') AS city,
    'OR' AS state,
    zip_code,
    NULL AS affiliation,
    NULL AS district,
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS insert_datetime

FROM parsed_address

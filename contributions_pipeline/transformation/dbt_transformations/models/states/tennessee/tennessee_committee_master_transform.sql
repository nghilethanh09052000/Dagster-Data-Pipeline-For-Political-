{{
    config(
      materialized='table',
      tags=["tennessee", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        -- Full name need to be constructed this way 
        -- as the contributions data also constructed 
        -- it this way, with `{first_name}(, {last_name})` format
        first_name
        || CASE
            WHEN
                last_name IS NOT NULL AND TRIM(last_name) != ''
                THEN ', ' || last_name
        END AS full_name,
        address_1,
        address_2,
        city,
        "state",
        zip,
        party_affiliation
    FROM {{ source('tn', 'tn_candidates_landing') }}
    WHERE
        first_name IS NOT NULL
        AND TRIM(first_name) != ''
),

transformed_data AS (
    SELECT
        'TN' AS "source",
        'TN_' || {{ dbt_utils.generate_surrogate_key(['full_name', 'city', 'zip', 'party_affiliation']) }} AS committee_id,
        NULL AS committee_designation,
        full_name AS "name",
        address_1 AS address1,
        address_2 AS address2,
        city,
        "state",
        zip AS zip_code,
        party_affiliation AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY full_name, city, zip, party_affiliation
        ) AS row_num
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
FROM transformed_data
WHERE row_num = 1

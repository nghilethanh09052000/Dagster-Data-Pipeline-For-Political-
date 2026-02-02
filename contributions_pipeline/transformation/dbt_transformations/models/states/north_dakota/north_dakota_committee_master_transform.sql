{{ config(
    materialized='table',
    tags=["north_dakota", "committee", "master"]
) }}

WITH source_data AS (
    SELECT
        year,
        committee_name,
        street,
        city,
        state,
        zip,
        committee_type
    FROM {{ source('nd', 'nd_committees_landing') }}
)

, cleaned_data AS (
    SELECT
        committee_name,
        MIN(year) AS first_year,
        MIN(street) AS street,
        MIN(city) AS city,
        MIN(state) AS state,
        MIN(zip) AS zip,
        MIN(committee_type) AS committee_type
    FROM source_data
    WHERE committee_name IS NOT NULL AND committee_name <> ''
    GROUP BY committee_name
)

, final AS (
    SELECT
        'ND' AS source,
        {{ generate_committee_id('ND_', 'committee_name') }} AS committee_id,
        NULL AS committee_designation,
        
        committee_name AS name,
        street AS address1,
        NULL AS address2,
        city,
        state,
        zip AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM cleaned_data
)

SELECT * FROM final
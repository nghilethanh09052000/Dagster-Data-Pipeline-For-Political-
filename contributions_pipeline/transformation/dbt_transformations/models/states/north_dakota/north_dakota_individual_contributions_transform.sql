{{ config(
    materialized='table',
    tags=["north_dakota", "contributions", "master"]
) }}

WITH source_data AS (
    SELECT
        contributor,
        street,
        city,
        state,
        zip,
        date,
        amount,
        contributed_to
    FROM {{ source('nd', 'nd_contributions_landing') }}
    WHERE contributor IS NOT NULL AND contributor <> ''
      AND contributed_to IS NOT NULL AND contributed_to <> ''
      AND amount IS NOT NULL AND amount <> ''
)

, cleaned_data AS (
    SELECT
        'ND' AS source,
        'ND_' || {{ dbt_utils.generate_surrogate_key([
            'contributor', 'street', 'city', 'date', 'amount', 'contributed_to',
        ]) }} AS source_id,

        {{ generate_committee_id('ND_', 'contributed_to') }} AS committee_id,

        contributor AS name,
        city AS city,
        state AS state,
        zip AS zip_code,
        NULL AS employer,
        NULL AS occupation,
        amount::decimal(10,2) AS amount,
        NULLIF(date, '')::timestamp AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    
)

SELECT DISTINCT * FROM cleaned_data

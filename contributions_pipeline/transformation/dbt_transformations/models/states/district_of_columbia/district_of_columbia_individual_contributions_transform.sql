{{
    config(
      materialized='table',
      tags=["district_of_columbia", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "COMMITTEENAME" AS committee_name,
        "CONTRIBUTORNAME" AS contributor_name,
        "EMPLOYER" AS employer,
        "AMOUNT" AS amount,
        "DATEOFRECEIPT" AS date_of_receipt,
        "LATITUDE" AS latitude,
        "LONGITUDE" AS longitude,
        "OBJECTID" AS object_id,
        "ADDRESS" AS city,
        CASE 
            WHEN "ADDRESS" IS NOT NULL AND "ADDRESS" <> '' THEN 
                REGEXP_SUBSTR("ADDRESS", '[A-Z]{2}', 1, 1)
            ELSE NULL 
        END AS state,
        CASE 
            WHEN "ADDRESS" IS NOT NULL AND "ADDRESS" <> '' THEN 
                REGEXP_SUBSTR("ADDRESS", '[0-9]{5}$')
            ELSE NULL 
        END AS zip_code
    FROM {{ source('dc', 'dc_financial_contributions_landing_table') }}
    WHERE
        "AMOUNT" IS NOT NULL AND "AMOUNT" <> ''
        AND "COMMITTEENAME" IS NOT NULL AND "COMMITTEENAME" <> ''
)

, cleaned AS (
    SELECT
        'DC' AS source,
        'DC_' || object_id AS source_id,
        {{ generate_committee_id('DC_', 'committee_name') }} AS committee_id,
        contributor_name AS name,
        city AS city,
        state AS state,
        zip_code AS zip_code,
        employer AS employer,
        NULL AS occupation,
        CAST(amount AS NUMERIC) AS amount,
        CAST(date_of_receipt AS TIMESTAMP) AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT * FROM cleaned

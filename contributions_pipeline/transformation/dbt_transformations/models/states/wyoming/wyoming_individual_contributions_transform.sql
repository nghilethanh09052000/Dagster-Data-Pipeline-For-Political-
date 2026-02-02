{{ 
    config(
        materialized='table',
        tags=["wyoming", "contributions", "master"]
    )
}}

WITH source AS (
    SELECT DISTINCT
        "Contributor Name",
        "Recipient Name",
        "Recipient Type",
        "Contribution Type",
        "Date",
        "Amount",
        "City State Zip",
        TRIM(SPLIT_PART("City State Zip", ',', 1)) AS city,
        TRIM(SPLIT_PART(SPLIT_PART("City State Zip", ',', 2), ' ', 1)) AS "state",
        TRIM(SPLIT_PART(SPLIT_PART("City State Zip", ',', 2), ' ', 2)) AS zip_code
    FROM {{ source('wy', 'wy_contributions_landing_table') }}
    WHERE
        ("Contribution Type" = 'MONETARY' OR "Contribution Type" = 'IN-KIND')
        AND "Contributor Name" IS NOT NULL AND TRIM("Contributor Name") != ''
        AND "Filing Status" = 'FILED'
),

transformed AS (
    SELECT
        'WY' AS "source",
        'WY_' || {{ dbt_utils.generate_surrogate_key(
            [
                '"Contributor Name"',
                '"Recipient Name"',
                '"Recipient Type"',
                '"Contribution Type"',
                '"Date"',
                '"Amount"',
                '"City State Zip"'
            ]
        ) }} AS source_id,
        'WY_' || {{ dbt_utils.generate_surrogate_key(['"Recipient Name"', '"Recipient Type"']) }} AS committee_id,
        "Contributor Name" AS name,
        city,
        "state",
        zip_code,
        NULL AS employer,
        NULL AS occupation,
        "Amount"::decimal AS amount,
        TO_TIMESTAMP("Date", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source
)

SELECT * FROM transformed

{{
    config(
        materialized='table',
        tags=["rhode_island", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "ContributionID",
        "OrganizationName",
        "FullName",
        "CityStZip",
        "EmployerName",
        "EmpAddress",
        "EmpCityStZip",
        "Amount",
        "ReceiptDate",
        ROW_NUMBER() OVER (
            PARTITION BY "ContributionID", "FullName", "Amount"
            ORDER BY "ReceiptDate"
        ) AS row_num
    FROM {{ source('ri', 'ri_contributions_landing') }}
    WHERE "TransType" = 'Contribution'
),

transformed_data AS (
    SELECT
        'RI' AS "source",
        'RI_' || {{ dbt_utils.generate_surrogate_key(['"ContributionID"', '"FullName"', '"Amount"']) }} AS source_id,
        'RI_' || {{ dbt_utils.generate_surrogate_key(['"OrganizationName"']) }} AS committee_id,
        "FullName" AS "name",
        -- Contributor Location
        TRIM(SPLIT_PART("CityStZip", ',', 1)) AS city,
        TRIM(SPLIT_PART(SPLIT_PART("CityStZip", ',', 2), ' ', 2)) AS "state",
        CASE
            WHEN SPLIT_PART(SPLIT_PART("CityStZip", ',', 2), ' ', 3) ~ '^\d+$'
                THEN SPLIT_PART(SPLIT_PART("CityStZip", ',', 2), ' ', 3)
            ELSE NULL
        END AS zip_code,
        -- Employer Information
        "EmployerName" AS employer,
        TRIM(SPLIT_PART("EmpAddress", ',', 1)) AS employer_address1,
        CASE
            WHEN TRIM(SPLIT_PART("EmpAddress", ',', 2)) != ''
                THEN TRIM(SPLIT_PART("EmpAddress", ',', 2))
            ELSE NULL
        END AS employer_address2,
        TRIM(SPLIT_PART("EmpCityStZip", ',', 1)) AS employer_city,
        TRIM(SPLIT_PART(SPLIT_PART("EmpCityStZip", ',', 2), ' ', 2))
            AS employer_state,
        CASE
            WHEN
                SPLIT_PART(SPLIT_PART("EmpCityStZip", ',', 2), ' ', 3) ~ '^\d+$'
                THEN SPLIT_PART(SPLIT_PART("EmpCityStZip", ',', 2), ' ', 3)
            ELSE NULL
        END AS employer_zip,
        -- Financial & Metadata
        NULL AS occupation,
        {{ clean_dolar_sign_amount('"Amount"') }} AS amount,
        TO_TIMESTAMP("ReceiptDate", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE row_num = 1
)

SELECT
    "source",
    source_id,
    committee_id,
    "name",
    city,
    "state",
    zip_code,
    employer,
    employer_address1,
    employer_address2,
    employer_city,
    employer_state,
    employer_zip,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM transformed_data


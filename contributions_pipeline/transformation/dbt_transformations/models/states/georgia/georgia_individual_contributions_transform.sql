{{
    config(
      materialized='table',
      tags=["georgia", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "Filer_ID",
        "Transaction_Amount",
        "Transaction_Date",
        "Last_Name",
        "First_Name",
        "Middle_Name",
        "Contributor_City",
        "Contributor_State",
        "Contributor_Zip_Code",
        "Transaction_ID",
        "Contributor_Employer",
        "Contributor_Occupation"
    FROM {{ source('ga', 'ga_contributions_and_loans_landing') }}
    WHERE
        "Filer_ID" IS NOT NULL AND "Filer_ID" <> ''
        AND "Transaction_Amount" IS NOT NULL AND "Transaction_Amount" <> ''
),

cleaned AS (
    SELECT
        'GA' AS source,
        'GA_' || "Transaction_ID" AS source_id,
        'GA_' || "Filer_ID" AS committee_id,
        CONCAT("First_Name", "Middle_Name", "Last_Name") AS name,
        "Contributor_City" AS city,
        "Contributor_State" AS state,
        "Contributor_Zip_Code" AS zip_code,
        "Contributor_Employer" AS employer,
        "Contributor_Occupation" AS occupation,
        CAST("Transaction_Amount" AS DECIMAL(9, 2)) AS amount,
        TO_TIMESTAMP("Transaction_Date", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT * FROM cleaned

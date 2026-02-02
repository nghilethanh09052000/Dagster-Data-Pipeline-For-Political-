{{
    config(
        materialized='table',
        tags=["colorado", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'CO' AS source,
        'CO_' || "RecordID" AS source_id,
        'CO_' || "CO_ID" AS committee_id,

        -- Candidate fields
        CASE 
            WHEN "CandidateName" IS NOT NULL AND "CandidateName" <> '' 
                THEN 'CO_' || "CO_ID" 
            ELSE NULL
        END AS candidate_id,
        NULLIF("CandidateName", '') AS candidate_name,

        -- Contributor fields
        COALESCE(NULLIF("FirstName", '') || ' ', '') || "LastName" AS name,
        "City" AS city,
        "State" AS state,
        "Zip" AS zip_code,
        "Employer" AS employer,
        "Occupation" AS occupation,
        "ContributionAmount"::decimal AS amount,
        to_timestamp("ContributionDate", 'YYYY-MM-DD HH24:MI:SS') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,

        ROW_NUMBER() OVER (
            PARTITION BY "RecordID"
        ) AS row_num
    FROM {{ source('co', 'co_contributions_landing') }}
    WHERE 
        "CO_ID" <> ''
        AND "ContributionAmount" IS NOT NULL 
        AND TRIM("ContributionAmount") <> ''
        AND LOWER(TRIM("ContributorType")) LIKE '%individual%'
)

SELECT
    source,
    source_id,
    committee_id,
    candidate_id,
    candidate_name,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM source_data
WHERE row_num = 1

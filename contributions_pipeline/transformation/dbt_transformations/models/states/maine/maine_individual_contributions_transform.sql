{{
    config(
        materialized='table',
        tags=["maine", "contributions", "master"]
    )
}}

WITH source_data_old AS (
    SELECT
        "OrgID",
        COALESCE(NULLIF("FirstName", '') || ' ', '')
        || "LastName"
        || CASE
            WHEN
                "Suffix" IS NOT NULL AND TRIM("Suffix") = ''
                THEN ' ' || "Suffix"
            ELSE ''
        END AS full_name,
        "ReceiptAmount",
        "ReceiptDate",
        "City",
        "State",
        "Zip",
        "ReceiptID",
        "CommitteeType",
        "Employer",
        CASE
            WHEN "Occupation" = 'Other' THEN "OccupationComment" ELSE
                "Occupation"
        END AS occupation
    FROM {{ source('me', 'maine_old_contributions_and_loans_landing') }}
    WHERE
        "ReceiptSourceType" = 'Individual'
        AND "Amended" = 'N'
),

source_data_new AS (
    SELECT
        "OrgID",
        COALESCE(NULLIF("FirstName", '') || ' ', '')
        || COALESCE(NULLIF("MiddleName", '') || ' ', '')
        || "LastName"
        || CASE
            WHEN
                "Suffix" IS NOT NULL AND TRIM("Suffix") = ''
                THEN ' ' || "Suffix"
            ELSE ''
        END AS full_name,
        "ReceiptAmount",
        "ReceiptDate",
        "City",
        "State",
        "Zip",
        "ReceiptID",
        "CommitteeType",
        "Employer",
        CASE
            WHEN "Occupation" = 'Other' THEN "OccupationComment" ELSE
                "Occupation"
        END AS occupation
    FROM {{ source('me', 'maine_new_contributions_and_loans_landing') }}
    WHERE
        "ReceiptSourceType" = 'Individual'
        AND "Amended" = 'N'
),

transformed_data AS (
    SELECT
        'ME' AS "source",
        'ME_OLD_' || "ReceiptID" AS source_id,
        'ME_OLD_' || "CommitteeType" || '_' || "OrgID" AS committee_id,
        full_name AS "name",
        "City" AS city,
        "State" AS "state",
        "Zip" AS zip_code,
        "Employer" AS employer,
        occupation,
        "ReceiptAmount"::decimal AS amount,
        TO_TIMESTAMP("ReceiptDate", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data_old

    UNION ALL

    SELECT
        'ME' AS "source",
        'ME_NEW_' || "ReceiptID" AS source_id,
        'ME_NEW_' || "CommitteeType" || '_' || "OrgID" AS committee_id,
        full_name AS "name",
        "City" AS city,
        "State" AS "state",
        "Zip" AS zip_code,
        "Employer" AS employer,
        occupation,
        "ReceiptAmount"::decimal AS amount,
        TO_TIMESTAMP("ReceiptDate", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data_new

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
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM transformed_data

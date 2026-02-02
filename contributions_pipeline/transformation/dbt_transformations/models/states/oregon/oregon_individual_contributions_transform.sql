{{ config(
    materialized='table',
    tags=["oregon", "contributions", "master"]
) }}

WITH source_data AS (
    SELECT
        "TranId",
        "Filer",
        "FilerId",
        "ContributorOrPayee",
        "City",
        "State",
        "ZipPlusFour",
        "EmpName",
        "OccptnTxt",
        "Amount",
        "TranDate"
    FROM {{ source('or', 'or_transaction_landing') }}
),

cleaned AS (
    SELECT
        'OR' AS "source",
        'OR_' || "TranId" AS source_id,
        'OR_' || "FilerId" AS committee_id,
        "ContributorOrPayee" AS "name",
        "City" AS city,
        "State" AS "state",
        "ZipPlusFour" AS zip_code,
        "EmpName" AS employer,
        "OccptnTxt" AS occupation,
        "Amount"::decimal AS amount,
        to_timestamp("TranDate", 'MM/DD/YYYY') AS contribution_datetime,
        current_timestamp AS insert_datetime
    FROM source_data
)

SELECT * FROM cleaned

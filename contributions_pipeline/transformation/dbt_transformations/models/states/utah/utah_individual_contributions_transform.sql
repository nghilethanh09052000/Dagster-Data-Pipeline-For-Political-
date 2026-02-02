{{
    config(
      materialized='table',
      tags=["utah", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "ENTITY",
        "TRAN_ID",
        "TRAN_DATE",
        "TRAN_AMT",
        "NAME",
        "CITY",
        "STATE",
        "ZIP"
    FROM {{ source('ut', 'ut_finance_report_landing_table') }}
    WHERE
        "TRAN_TYPE" = 'Contribution'
        AND ("AMENDS" IS NULL OR TRIM("AMENDS") = '')
),

transformed_data AS (
    SELECT
        'UT' AS "source",
        'UT_' || "TRAN_ID" AS source_id,
        'UT_' || {{ dbt_utils.generate_surrogate_key(['"ENTITY"']) }} AS committee_id,
        "NAME" AS "name",
        "CITY" AS city,
        "STATE" AS "state",
        "ZIP" AS zip_code,
        NULL AS employer,
        NULL AS occupation,
        "TRAN_AMT"::decimal AS amount,
        TO_TIMESTAMP("TRAN_DATE", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    source_id,
    committee_id,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM transformed_data
WHERE amount >= 0

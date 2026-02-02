{{
    config(
      materialized='table',
      tags=["utah", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT "ENTITY"
    FROM {{ source('ut', 'ut_finance_report_landing_table') }}
    GROUP BY
        "ENTITY"
),

transformed_data AS (
    SELECT
        'UT' AS "source",
        'UT_' || {{ dbt_utils.generate_surrogate_key(['"ENTITY"']) }} AS committee_id,
        NULL AS committee_designation,
        "ENTITY" AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

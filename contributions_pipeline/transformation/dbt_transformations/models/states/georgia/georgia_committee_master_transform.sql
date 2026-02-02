{{
    config(
      materialized='table',
      tags=["georgia", "committee", "master"]
    )
}}

{% set static_columns = [
    "'GA' as source",
    "('GA_' || \"Filer_ID\") as committee_id",
    "NULL as committee_designation",
    "\"Committee_Name\" as name",
    "NULL as address1",
    "NULL as address2",
    "NULL as city",
    "NULL as state",
    "NULL as zip_code",
    "NULL as affiliation",
    "NULL as district",
    "CURRENT_TIMESTAMP as insert_datetime"
] %}

WITH contribution_src AS (
    SELECT DISTINCT
        {{ static_columns | join(',\n        ') }}
    FROM {{ source('ga', 'ga_contributions_and_loans_landing') }}
    WHERE TRIM("Committee_Name") != ''
),

expenditures_src AS (
    SELECT DISTINCT
        {{ static_columns | join(',\n        ') }}
    FROM {{ source('ga', 'ga_expenditures_landing') }}
    WHERE TRIM("Committee_Name") != ''
),

union_data AS (
    SELECT * FROM contribution_src
    UNION ALL
    SELECT * FROM expenditures_src
)

SELECT DISTINCT * FROM union_data

{{
    config(
      materialized='table',
      tags=["south_carolina", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        contribution_id,
        office_run_id,
        candidate_id,
        "date",
        amount,
        candidate_name,
        contributor_name,
        contributor_occupation,
        contributor_address,
        SUBSTRING(
            contributor_address FROM '([A-Z]{2})(\s|,\s)[0-9]{5}(-[0-9]{4})?'
        ) AS contributor_state,
        SUBSTRING(
            contributor_address FROM '.*\s([0-9]{3,5}(-[0-9]{4})?)(\s|$)'
        ) AS contributor_zip_code
    FROM {{ source('sc', 'sc_contributions_landing') }}
),

transformed_data AS (
    SELECT
        'SC' AS "source",
        'SC_' || contribution_id AS source_id,
        'SC_' || candidate_id || '_' || office_run_id AS committee_id,
        contributor_name AS "name",
        NULL AS city,
        contributor_state AS "state",
        contributor_zip_code AS zip_code,
        NULL AS employer,
        contributor_occupation AS occupation,
        amount::decimal AS amount,
        "date"::timestamp AS contribution_datetime,
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

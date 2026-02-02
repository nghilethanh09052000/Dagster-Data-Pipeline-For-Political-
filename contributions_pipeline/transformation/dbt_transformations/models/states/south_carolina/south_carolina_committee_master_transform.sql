{{
    config(
      materialized='table',
      tags=["south_carolina", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        office_run_id,
        candidate_id,
        candidate_name
    FROM {{ source('sc', 'sc_contributions_landing') }}
    GROUP BY
        office_run_id,
        candidate_id,
        candidate_name
),

cleaned_data AS (
    SELECT
        'SC' AS "source",
        'SC_' || candidate_id || '_' || office_run_id AS committee_id,
        NULL AS committee_designation,
        candidate_name AS "name",
        NULL AS address1, -- No address available from the exported data
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
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM cleaned_data

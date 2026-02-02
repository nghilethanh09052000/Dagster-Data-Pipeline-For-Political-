{{
    config(
      materialized='table',
      tags=["florida", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT DISTINCT "Candidate/Committee" AS committee_name
    FROM {{ source('fl', 'fl_contributions_landing') }}
),

cleaned_data AS (
    SELECT
        'FL' AS "source",
        {{ generate_committee_id('FL_', 'committee_name') }} AS committee_id,
        NULL AS committee_designation,
        committee_name AS "name",
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

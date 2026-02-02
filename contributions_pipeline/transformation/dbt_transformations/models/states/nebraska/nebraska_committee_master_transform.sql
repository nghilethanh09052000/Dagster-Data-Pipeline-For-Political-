{{
    config(
      materialized='table',
      tags=["nebraska", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        org_id,
        filer_type,
        filer_name,
        MAX(NULLIF(TRIM(candidate_name), '')) AS candidate_name
    FROM {{ source('ne', 'ne_contributions_loan_landing') }}
    GROUP BY
        org_id,
        filer_type,
        filer_name
),

transformed_data AS (
    SELECT
        'NE' AS "source",
        'NE_' || org_id AS committee_id,
        -- Candidate id only when this is a candidate committee
        CASE 
            WHEN filer_type = 'Candidate Committee'
                 AND org_id IS NOT NULL AND TRIM(org_id) <> ''
            THEN 'NE_' || org_id
            ELSE NULL
        END AS candidate_id,
        candidate_name,
        CASE
            WHEN filer_type = 'Candidate Committee' THEN 'P'
            WHEN filer_type = 'PAC-Separate Segregated Political Fund' THEN 'B'
            WHEN filer_type = 'PAC-Independent' THEN 'U'
            WHEN filer_type = 'Ballot Question Committee' THEN 'U'
            WHEN filer_type = 'Political Party Committee' THEN 'U'
        END AS committee_designation,
        filer_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        -- There's a possibility that the organization
        -- have minor typo between the types,
        -- but each org_id should be be unique against
        -- minor difference of the name
        ROW_NUMBER() OVER (
            PARTITION BY org_id
        ) AS row_num
    FROM source_data
)

SELECT
    source,
    committee_id,
    candidate_id,
    candidate_name,
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
WHERE
    row_num = 1

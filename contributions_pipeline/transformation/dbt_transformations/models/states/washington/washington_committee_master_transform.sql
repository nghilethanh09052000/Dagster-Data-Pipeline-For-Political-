{{
    config(
      materialized='table',
      tags=["washington", "committee", "master"]
    )
}}

-- Landing table docs:
-- https://www.pdc.wa.gov/political-disclosure-reporting-data/open-data/dataset/Campaign-Finance-Summary

WITH source_data AS (
    SELECT
        committee_id,
        filer_name,
        committee_address,
        committee_city,
        committee_state,
        committee_zip,
        legislative_district,
        committee_category,
        party,
        -- Each filer ident could be multiple type, but we'll just take one
        ROW_NUMBER() OVER (
            PARTITION BY committee_id
        ) AS row_num
    FROM {{ source('wa', 'wa_campaign_finance_summary_landing') }}
),

transformed_data AS (
    SELECT
        'WA' AS "source",
        'WA_' || committee_id AS committee_id,
        CASE
            WHEN committee_category = 'Candidate' THEN 'WA_' || committee_id
        END AS candidate_id,
        -- Probably faster than just joining the table to the candidate table
        -- Plus it means both table could be done in parallel!
        CASE
            WHEN
                committee_category = 'Candidate'
                THEN TRIM(REGEXP_REPLACE(filer_name, '\([^()]*\)', ''))
        END AS candidate_name,
        CASE
            WHEN committee_category = 'Candidate' THEN 'P'
            WHEN
                committee_category IN (
                    'Democratic State Party',
                    'Republican State Party',
                    'Democratic County Party',
                    'Republican County Party',
                    'Democratic Leg District Party',
                    'Republican Leg District Party',
                    'Democratic Caucus',
                    'Republican Caucus',
                    'Democratic Caucus Affiliated',
                    'Republican Caucus Affiliated',
                    'Minor Party'
                )
                THEN 'U'
            WHEN
                committee_category IN (
                    'Single Election', 'Single Election Committee'
                )
                THEN 'U'
            WHEN
                committee_category IN (
                    'Continuing Committee (Union)',
                    'Continuing Committee (Business)',
                    'Continuing Committee'
                )
                THEN 'U'
            WHEN
                committee_category IN (
                    'Out-of-state political committee', 'Out-of-State'
                )
                THEN 'U'
            WHEN committee_category IN ('Education') THEN 'U'
            WHEN
                committee_category IN (
                    'Issue', 'Local Ballot Measure', 'Statewide Ballot Measure'
                )
                THEN 'U'
        END AS committee_designation,
        filer_name AS "name",
        committee_address AS address1,
        NULL AS address2,
        committee_city AS city,
        committee_state AS "state",
        committee_zip AS zip_code,
        party AS affiliation,
        legislative_district AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE row_num = 1
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

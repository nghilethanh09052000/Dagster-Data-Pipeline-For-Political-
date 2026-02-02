{{
    config(
      materialized='table',
      tags=["nebraska", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        receipt_id,
        org_id,
        receipt_date,
        receipt_amount,
        candidate_name,
        filer_type,
        COALESCE(NULLIF(suffix, '') || ' ', '')
        || first_name
        || COALESCE(NULLIF(middle_name, '') || ' ', '')
        || COALESCE(NULLIF(contributor_or_source_last_name, '') || ' ', '')
            AS full_name,
        address_1,
        address_2,
        city,
        "state",
        zip,
        employer,
        occupation,
        -- One receipt_id done by a single filer could be
        -- for multiple candidate (weird), but the amount is the same
        -- so in a way it could be a single contributions, might
        -- need to change it if it's not that way
        ROW_NUMBER() OVER (
            PARTITION BY receipt_id
        ) AS row_num
    FROM {{ source('ne', 'ne_contributions_loan_landing') }}
    WHERE
        contributor_or_transaction_source_type = 'Individual'
        AND amended = 'N'
),

transformed_data AS (
    SELECT
        'NE' AS "source",
        'NE_' || receipt_id AS source_id,
        'NE_' || org_id AS committee_id,
        -- Candidate id only for candidate committees
        CASE 
            WHEN filer_type = 'Candidate Committee'
                 AND org_id IS NOT NULL AND TRIM(org_id) <> ''
            THEN 'NE_' || org_id
            ELSE NULL
        END AS candidate_id,
        -- Candidate name only when candidate committee and present
        CASE 
            WHEN filer_type = 'Candidate Committee' AND NULLIF(TRIM(candidate_name), '') IS NOT NULL
            THEN TRIM(candidate_name)
            ELSE NULL
        END AS candidate_name,
        full_name AS "name",
        city,
        "state",
        zip AS zip_code,
        employer,
        occupation,
        receipt_amount::decimal AS amount,
        TO_TIMESTAMP(receipt_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE
        row_num = 1
)

SELECT
    source,
    source_id,
    committee_id,
    candidate_id,
    candidate_name,
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

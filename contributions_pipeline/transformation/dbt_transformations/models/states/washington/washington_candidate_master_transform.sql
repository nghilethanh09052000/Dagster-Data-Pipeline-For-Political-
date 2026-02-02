{{
    config(
      materialized='table',
      tags=["washington", "candidate", "master"]
    )
}}

-- Landing table docs:
-- https://www.pdc.wa.gov/political-disclosure-reporting-data/open-data/dataset/Campaign-Finance-Summary

WITH source_data AS (
    SELECT
        committee_id,
        -- Over ~45% of candidate filer name contains their
        -- "nickname" inside of parentheses, this should remove
        -- it and only returns with the full name
        TRIM(REGEXP_REPLACE(filer_name, '\([^()]*\)', '')) AS "name",
        party,
        -- Each filer ident could be multiple type, but we'll just take one
        ROW_NUMBER() OVER (
            PARTITION BY committee_id
        ) AS row_num
    FROM {{ source('wa', 'wa_campaign_finance_summary_landing') }}
    WHERE committee_category = 'Candidate'
)

SELECT
    'WA' AS "source",
    -- Should be okay to re-use the committee_id as candidate_id
    -- as it's one-to-one representation
    'WA_' || committee_id AS candidate_id,
    "name",
    party AS "affiliation",
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE row_num = 1

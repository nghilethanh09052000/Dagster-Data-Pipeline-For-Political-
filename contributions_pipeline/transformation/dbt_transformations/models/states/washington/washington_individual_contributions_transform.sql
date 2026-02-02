{{
    config(
      materialized='incremental',
      unique_key='source_id',
      incremental_strategy='delete+insert',
      tags=["washington", "contributions", "master"]
    )
}}

-- Landing table docs:
-- https://www.pdc.wa.gov/political-disclosure-reporting-data/open-data/dataset/Contributions-to-Candidates-and-Political-Committees

WITH source_data AS (
    SELECT
        id,
        committee_id,
        amount,
        receipt_date,
        contributor_name,
        contributor_city,
        contributor_state,
        contributor_zip,
        contributor_occupation,
        contributor_employer_name
    FROM {{
        source(
            'wa',
            'wa_contributions_to_candidates_and_political_committees_landing'
        ) }}
    WHERE
        -- Filter by forms that represents
        -- cash contributions (not in-kind or auction result)
        origin = 'C3'
        AND contributor_category = 'Individual'
),

transformed_data AS (
    SELECT
        'WA' AS "source",
        'WA_' || id AS source_id,
        'WA_' || committee_id AS committee_id,
        contributor_name AS "name",
        contributor_city AS city,
        contributor_state AS "state",
        contributor_zip AS zip_code,
        contributor_employer_name AS employer,
        contributor_occupation AS occupation,
        amount::decimal AS amount,
        TO_TIMESTAMP(receipt_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
),

enriched_data AS (
    SELECT
        cnt.source,
        cnt.source_id,
        cnt.committee_id,
        cm.candidate_id,
        cm.candidate_name,
        cnt.name,
        cnt.city,
        cnt.state,
        cnt.zip_code,
        cnt.employer,
        cnt.occupation,
        cnt.amount,
        cnt.contribution_datetime,
        cnt.insert_datetime
    FROM transformed_data AS cnt
    LEFT JOIN
        {{ ref('washington_committee_master_transform') }} AS cm
        ON cnt.committee_id = cm.committee_id
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
FROM enriched_data AS new_rows

{% if is_incremental() %}
    WHERE
        NOT EXISTS
        (
            SELECT 1
            FROM {{ this }} AS src
            WHERE src.source_id = new_rows.source_id
        )
{% endif %}

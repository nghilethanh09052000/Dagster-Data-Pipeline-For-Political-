{{
    config(
        materialized='table',
        tags=["connecticut", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        committee_name,
        candidate_chairperson,
        treasurer,
        deputy_treasurer,
        office_sought,
        committee_type,
        termination_date,
        first_registration_date,
        party_designation,
        district,
        city,
        state
    FROM {{ source('ct', 'ct_committees_landing') }}
)

, deduplicated AS (
    SELECT
        'CT' as source,

        {{ generate_committee_id('CT_', 'committee_name') }} AS committee_id,

        NULL as committee_designation,

        MIN(committee_name) as name,
        NULL as address1,
        NULL as address2,
        MIN(city) as city,
        MIN(state) as state,
        NULL as zip_code,
        MIN(party_designation) as affiliation,
        MIN(office_sought) as district,
        CURRENT_TIMESTAMP as insert_datetime

    FROM source_data
    WHERE committee_name IS NOT NULL
      AND committee_name <> ''
    GROUP BY committee_name
)

, final AS (
    SELECT DISTINCT * FROM deduplicated
)

SELECT * FROM final

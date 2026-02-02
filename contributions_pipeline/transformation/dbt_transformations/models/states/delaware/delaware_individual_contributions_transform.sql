{{
    config(
        materialized='table',
        tags=["delaware", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'DE' as source,
        'DE_' || {{ dbt_utils.generate_surrogate_key(
            [
                'cf_id', 
                'contribution_date', 
                'contributor_name', 
                'contributor_city',
                'contributor_state',
                'contributor_zip',
                'employer_name',
                'employer_occupation',
                'contribution_amount',
                'contribution_date'
            ]
        ) 
        }} as source_id,
        'DE_' || cf_id as committee_id,
        contributor_name as name,
        contributor_city as city,
        contributor_state as state,
        contributor_zip as zip_code,
        employer_name as employer,
        employer_occupation as occupation,
        contribution_amount::decimal(10,2) as amount,
        contribution_date::timestamp as contribution_datetime,
        CURRENT_TIMESTAMP as insert_datetime
    FROM {{ source('de', 'de_contributions_landing') }}
    WHERE 
        contribution_date <> ''
        and contribution_amount is not null and contribution_amount <> ''
        and cf_id is not null and cf_id <> ''
)

SELECT DISTINCT
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
FROM source_data 

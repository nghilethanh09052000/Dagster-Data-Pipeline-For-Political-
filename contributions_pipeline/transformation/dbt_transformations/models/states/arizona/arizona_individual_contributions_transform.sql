{{
    config(
        materialized='table',
        tags=["arizona", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        date,
        committee_name,
        amount,
        transaction_type,
        contribution_name
    FROM {{ source('az', 'az_contributions_landing') }}
)

, final AS (
    SELECT
        'AZ' as source,
        'AZ_' || {{
            dbt_utils.generate_surrogate_key(
                [
                    'date',
                    'committee_name',
                    'amount',
                    'transaction_type',
                    'contribution_name'
                ]
            )
        }} as source_id,
        {{
            generate_committee_id('AZ_', 'committee_name')
        }} as committee_id,
        committee_name as name,
        null as city,
        null as state,
        null as zip_code,
        null as employer,
        null as occupation,
        amount::decimal(10,2) as amount,
        cast(date as timestamp) as contribution_datetime,
        current_timestamp as insert_datetime
    FROM source_data
)

SELECT 
* 
FROM final 

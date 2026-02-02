{{ 
    config(
        materialized='table',
        tags=["kansas", "contributions", "master"]
    ) 
}}

with source as (
    select distinct
        candidate_name,
        contributor_name,
        contributor_address,
        contributor_city,
        contributor_state as city,
        contributor_zip as state,
        occupation as zip_code,
        industry as occupation,
        tender_type as contribution_datetime,
        in_kind_amount as amount
    from {{ source('ks', 'ks_contribution_landing') }}
    where 
        amount is not null and amount <> ''
        and candidate_name is not null and candidate_name <> ''

),

cleaned as (
    select
        'KS' as source,
        'KS_' || {{ dbt_utils.generate_surrogate_key(
                [
                    'candidate_name', 
                    'contributor_name', 
                    'contributor_address',
                    'contributor_city',
                    'city',
                    'state',
                    'zip_code',
                    'occupation',
                    'contribution_datetime',
                    'amount'
                ]
            ) 
        }} as source_id,
        {{ generate_committee_id('KS_', 'candidate_name') }} AS committee_id,
        contributor_name as name,
        city,
        state,
        zip_code,
        null as employer, 
        occupation,
        {{
            clean_dolar_sign_amount('amount')
        }} as amount,
        contribution_datetime::timestamp as contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    from source
)

select * from cleaned

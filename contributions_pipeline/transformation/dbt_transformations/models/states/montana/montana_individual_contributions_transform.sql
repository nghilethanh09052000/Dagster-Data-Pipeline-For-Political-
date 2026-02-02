{{
    config(
        materialized='table',
        tags=["montana", "individual_contributions", "transform"]
    )
}}

with committee_contributions as (
    select distinct
        'MT' as source,
        'MT_' || committee_id || '_' || transaction_id as source_id,
        'MT_' || committee_id as committee_id,
        null as candidate_id,  -- Committee contributions don't have direct candidate_id
        null as candidate_name,  -- Committee contributions don't have direct candidate_name
        contributor_name as name,
        contributor_city as city,
        contributor_state as state,
        regexp_matches(contributor_address, '(\d{5}(?:-\d{4})?)') as zip_match,
        nullif(occupation, '') as occupation,
        nullif(employer, '') as employer,
        {{
            clean_dolar_sign_amount('transaction_amount')
        }} as amount,
        transaction_date,
        election_year,
        current_timestamp as insert_datetime
    from {{ source('mt', 'mt_contributions_committees_details_landing') }}
    where transaction_type = 'Individual Contributions'
),

candidate_contributions as (
    select distinct
        'MT' as source,
        'MT_' || candidate_id || '_' || transaction_id as source_id,
        'MT_' || candidate_id as committee_id,  -- Use candidate_id as committee_id to avoid filtering out
        'MT_' || candidate_id as candidate_id,
        candidate_name,  -- Use actual candidate name from source data
        contributor_name as name,
        contributor_city as city,
        contributor_state as state,
        regexp_matches(contributor_address, '(\d{5}(?:-\d{4})?)') as zip_match,
        nullif(occupation, '') as occupation,
        nullif(employer, '') as employer,
        {{
            clean_dolar_sign_amount('transaction_amount')
        }} as amount,
        transaction_date,
        election_year,
        current_timestamp as insert_datetime
    from {{ source('mt', 'mt_contributions_candidates_details_landing') }}
    where transaction_type = 'Individual Contributions'
),

combined_contributions as (
    select * from committee_contributions
    union all
    select * from candidate_contributions
),

deduplicated_contributions as (
    select 
        *,
        row_number() over (
            partition by source_id 
            order by insert_datetime desc
        ) as rn
    from combined_contributions
),

added_contribution_time as (
    select
        source,
        source_id,
        committee_id,
        candidate_id,
        candidate_name,
        name,
        city,
        state,
        zip_match,
        occupation,
        employer,
        amount,
        transaction_date,
        election_year,
        insert_datetime,
        case
            when
                (
                    transaction_date is null
                    or trim(transaction_date) = ''
                )
                and election_year is not null
                and trim(election_year) <> ''
                then cast(trim(election_year) || '-01-01' as timestamp)
            when
                (transaction_date is not null and trim(transaction_date) <> '')
                then cast(transaction_date as timestamp)
        end as contribution_datetime,
        case
            when
                zip_match is not null and array_length(zip_match, 1) > 0
                then zip_match[1]
            else '00000'
        end as zip_code
    from deduplicated_contributions
    where rn = 1
)

select 
    source,
    source_id,
    committee_id,
    candidate_id,
    candidate_name,
    name,
    city,
    state,
    zip_code,
    occupation,
    employer,
    amount,
    contribution_datetime,
    insert_datetime
from added_contribution_time
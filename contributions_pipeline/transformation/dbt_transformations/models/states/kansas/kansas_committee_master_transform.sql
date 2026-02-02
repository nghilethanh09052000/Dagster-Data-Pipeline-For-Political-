{{ 
    config(
        materialized='table',
        tags=["kansas", "committee", "master"]
    ) 
}}

with source as (
    select
        min(date) as first_filed_date,
        committee_name,
        max(address) as address, 
        max(other) as other,
        max(city) as city,
        max(zip_code) as zip_code
    from {{ source('ks', 'ks_committees_landing') }}
    where committee_name is not null 
      and committee_name <> ''
    group by committee_name
)

, final as (
    select
        'KS' as source,
        {{ generate_committee_id('KS_', 'committee_name') }} as committee_id,
        null as committee_designation,
        committee_name as name,
        address as address1,
        other as address2,
        city,
        null as state,
        zip_code,
        null as affiliation,
        null as district,
        CURRENT_TIMESTAMP as insert_datetime
    from source
)

select * from final

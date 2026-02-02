{{ config(
    materialized='table',
    tags=["west_virginia", "committee", "master"]
) }}

with source as (
    select
        committee_name,
        committee_type,
        id_number,
        committee_address,
        town,
        party,
        district
    from {{ source('wv', 'wv_committees_landing') }}
),

cleaned as (
    select
        *,
        -- Extract ZIP code
        regexp_replace(committee_address, '.*(\d{5})(-\d{4})?$', '\1') as parsed_zip,
        -- Use town column for city
        town as parsed_city,
        -- Remove ZIP from address for address1
        regexp_replace(committee_address, ',? ?\d{5}(-\d{4})?$', '') as address_no_zip
    from source
),

final as (
    select
        'WV' as source,
        'WV_' || id_number as committee_id,
        null as committee_designation,

        committee_name as name,

        address_no_zip as address1,
        null as address2,
        parsed_city as city,
        'WV' as state,
        coalesce(nullif(parsed_zip, ''), '00000') as zip_code,
        party as affiliation,
        district,
        CURRENT_TIMESTAMP as insert_datetime
    from cleaned
)

select distinct * from final
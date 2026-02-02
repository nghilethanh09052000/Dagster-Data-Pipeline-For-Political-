{{ config(
    materialized='table',
    tags=["alaska", "individual_contributions", "combined"]
) }}


with source_contribution as (
    select 
        filer_name,
        filer_address,
        filer_city,
        filer_state,
        filer_zip
    from 
        {{ source('ak', 'ak_contribution_reports_landing') }}
    where 
        filer_name is not null and filer_name <> ''
),

contribution_transformed as (
    select
        'AK' as source,
        {{ generate_committee_id('AK_', 'filer_name') }} as committee_id,
        null as committee_designation,
        filer_name as name,
        filer_address as address1,
        null as address2,
        filer_city as city,
        filer_state as state,
        filer_zip as zip_code,
        null as affiliation,
        null as district,
        current_timestamp as insert_datetime
    from source_contribution
),

final as (
    select
        *,
        ROW_NUMBER() OVER (PARTITION BY committee_id) as row_num
    from contribution_transformed
)

select * from final where row_num = 1

{{
    config(
        materialized='table',
        tags=["hawaii", "committee", "master"]
    )
}}

with source_data as (
    select distinct
        "Noncandidate Committee Name" as noncandidate_committee_name
    from {{ source('hi', 'hi_contrib_recv_by_non_candidates_landing_table') }}
),

transformed as (
    select
        'HI' as source,
        {{ generate_committee_id('HI_', 'noncandidate_committee_name') }} as committee_id,
        null::text as committee_designation,
        noncandidate_committee_name as name,
        null::text as address1,
        null::text as address2,
        null::text as city,
        null::text as state,
        null::text as zip_code,
        null as affiliation,
        null as district,
        current_timestamp as insert_datetime
    from source_data
    where noncandidate_committee_name is not null 
      and noncandidate_committee_name <> 'Noncandidate Committee Name' -- remove header row
)

select * 
from transformed

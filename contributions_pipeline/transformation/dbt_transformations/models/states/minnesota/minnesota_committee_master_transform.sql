{{
    config(
        materialized='table',
        tags=["minnesota", "committee", "master"]
    )
}}

with all_committee as (
    select 
        "Committee reg num" as committee_id,
        "Committee name" as name
    from {{ source('mn', 'mn_general_expenditures_landing_table') }} 
    where "Entity type" in ('PCC', 'PCF')
    union
    select
        "Recipient reg num" as committee_id,
        "Recipient" as name
    from {{ source('mn', 'mn_contrib_landing_table') }}
    where "Recipient type" in ('PCC', 'PCF') 
)
select
    'MN' as source,
    'MN_' || committee_id as committee_id,
    NULL as committee_designation,
    name,
    null as address1,
    null as address2,
    null as city,
    null as state,
    null as zip_code,
    null as affiliation,
    null as district,
    CURRENT_TIMESTAMP as insert_datetime
from all_committee
{{
    config(
        materialized='table',
        tags=["missouri", "committee", "master"]
    )
}}

select
    'MO' as source,
    'MO_' || "MECID" as committee_id,
    NULL as committee_designation,
    "Committee Name" as name,
    null as address1,
    null as address2,
    null as city,
    null as state,
    null as zip_code,
    null as affiliation,
    null as district,
    CURRENT_TIMESTAMP as insert_datetime
from {{ source('mo', 'mo_committee_data_landing_table') }}
where "Committee Type" = 'Candidate'
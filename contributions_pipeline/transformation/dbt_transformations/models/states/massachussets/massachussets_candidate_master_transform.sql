{{
    config(
        materialized='table',
        tags=["massachussets", "candidate", "master"]
    )
}}

with candidate_data as (
    select
        'MA' as source,
        'MA_' || "CPF ID" as candidate_id,
        "Candidate First Name" || ' ' || "Candidate Last Name" as name,
        "Party Affiliation" as affiliation,
        "District Name Sought" as district,
        current_timestamp as insert_datetime
    from {{ source('ma', 'ma_committee_candidate_landing_table') }}
    where "Candidate First Name" <> ''
)

select *
from candidate_data

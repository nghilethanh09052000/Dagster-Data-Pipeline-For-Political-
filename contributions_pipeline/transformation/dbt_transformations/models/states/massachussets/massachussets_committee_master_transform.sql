{{
    config(
        materialized='table',
        tags=["massachussets", "committee", "master"]
    )
}}

with committee_data as (
    select
        'MA' as source,
        'MA_' || "CPF ID" as committee_id,
        case
            when "Candidate First Name" <> '' then 'MA_' || "CPF ID"
            else null
        end as candidate_id,
        trim(nullif("Candidate First Name", '')) || ' ' || trim(nullif("Candidate Last Name", '')) as candidate_name,
        null as committee_designation,
        case
            when
                trim("Comm_Name") = ''
                then "Candidate First Name" || ' ' || "Candidate Last Name"
            else "Comm_Name"
        end as name,
        "Candidate Street Address" as address1,
        null as address2,
        "Candidate City" as city,
        "Candidate State" as state,
        "Candidate Zip Code" as zip_code,
        "Party Affiliation" as affiliation,
        "District Name Sought" as district,
        current_timestamp as insert_datetime
    from {{ source('ma', 'ma_committee_candidate_landing_table') }}
)

select * from committee_data

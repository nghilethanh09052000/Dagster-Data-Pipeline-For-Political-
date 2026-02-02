{{
    config(
        materialized = 'table',
        tags = ["iowa", "committee", "master"]
    )
}}

with source_data as (
    select
        committee_name,
        committee_number,
        candidate_name,
        committee_type,
        district,
        party,
        candidate_address,
        candidate_city_state_zip,
        chair_city_state_zip,
        treasurer_city_state_zip
    from {{ source('ia', 'ia_registered_political_candidates_landing') }}
)

, mapped as (
    select
        'IA' as source,

        'IA_' || committee_number as committee_id,

        case 
            when nullif(candidate_name, '') is not null 
            then 'IA_' || committee_number
            else null
        end as candidate_id,

        nullif(candidate_name, '') as candidate_name,

        case committee_type
            -- Principal campaign committees
            when 'Attorney General' then 'P'
            when 'Auditor of State' then 'P'
            when 'City Candidate - City Council' then 'P'
            when 'City Candidate - Mayor' then 'P'
            when 'County Candidate - Attorney' then 'P'
            when 'County Candidate - Auditor' then 'P'
            when 'County Candidate - Hospital Trustee' then 'P'
            when 'County Candidate - Recorder' then 'P'
            when 'County Candidate - Sheriff' then 'P'
            when 'County Candidate - Supervisor' then 'P'
            when 'County Candidate - Treasurer' then 'P'
            when 'Governor' then 'P'
            when 'Lt Governor' then 'P'
            when 'Other Political Subdivision Candidate' then 'P'
            when 'School Board Candidate' then 'P'
            when 'Secretary of Agriculture' then 'P'
            when 'Secretary of State' then 'P'
            when 'State House' then 'P'
            when 'State Senate' then 'P'
            when 'Treasurer of State' then 'P'

            -- Party committees
            when 'County Central Committee' then 'U'
            when 'State Central Committee' then 'U'

            -- PACs / Ballot / Others (unauthorized by candidate)
            when 'City PAC' then 'U'
            when 'County PAC' then 'U'
            when 'Iowa PAC' then 'U'
            when 'Local Ballot Issue' then 'U'
            when 'School Board or Other Political Subdivision PAC' then 'U'

            else 'U' -- default to unauthorized if unknown
        end as committee_designation,

        committee_name as name,
        candidate_address as address1,
        null as address2,

        coalesce(candidate_city_state_zip, chair_city_state_zip, treasurer_city_state_zip) as raw_city_state_zip,

        split_part(trim(coalesce(candidate_city_state_zip, chair_city_state_zip, treasurer_city_state_zip)), ',', 1) as city,
        trim(split_part(coalesce(candidate_city_state_zip, chair_city_state_zip, treasurer_city_state_zip), ',', 3)) as zip_code,

        null as state,
        party as affiliation,
        district as district,
        current_timestamp as insert_datetime

    from source_data
    where committee_name is not null
      and committee_name <> ''
)

, final as (
    select
        source,
        committee_id,
        candidate_id,
        candidate_name,
        committee_designation,
        name,
        address1,
        address2,
        city,
        state,
        zip_code,
        affiliation,
        district,
        insert_datetime
    from mapped
)

select * from final

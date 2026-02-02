{{
    config(
      materialized='table',
      tags=["virginia", "candidates", "master"]
    )
}}

-- Data docs: https://www.elections.virginia.gov/candidatepac-info/political-committees/

with source_data as (
    select
        'VA' as "source",
        'VA_' || "VoterRegistrationid" as candidate_id,
        trim(
            coalesce(nullif("CandidateFirstName", '') || ' ', '')
            || coalesce(nullif("CandidateMiddleName", '') || ' ', '')
            || "CandidateLastName"
        ) as "name",
        "PoliticalPartyName" as affiliation,
        row_number() over (
            partition by "VoterRegistrationid"
        ) as row_num
    from {{ source('va', 'va_candidate_campaign_committee_landing') }}
    where
        -- VA does some smoke test in prod, this is the candidate id they use
        "VoterRegistrationid" != '101025435'

    union all

    -- Schedule b old format contains non-PAC data, so it should be
    -- safe to assume whoever it is on the "officer" position, in
    -- this case either "CandOfficer" or "Committee Treasurer" is
    -- the candidate itself.
    select
        'VA' as "source",
        'VA_' || {{ dbt_utils.generate_surrogate_key(['first_name', 'middle_name', 'last_name']) }} as candidate_id,
        trim(
            coalesce(nullif(first_name, '') || ' ', '')
            || coalesce(nullif(middle_name, '') || ' ', '')
            || last_name
        ) as "name",
        party as affiliation,
        row_number() over (
            partition by first_name, middle_name, last_name
        ) as row_num
    from {{ source('va', 'scheduleb_old_landing') }}
)

select
    "source",
    candidate_id,
    "name",
    affiliation,
    current_timestamp as insert_datetime
from source_data
where row_num = 1

{{
    config(
      materialized='table',
      tags=["illinois", "committee", "master"]
    )
}}

-- Landing tables docs: https://elections.il.gov/campaigndisclosuredatafiles/CampaignDisclosureDataDictionary.txt

with source_data as (
    select
        "ID",
        "TypeOfCommittee",
        "Name",
        "Address1",
        "Address2",
        "City",
        "State",
        "Zip",
        "PartyAffiliation"
    from {{ source('il', 'il_committees_landing') }}
),
joined as (
    select
        s."ID" as committee_id_raw,
        s."TypeOfCommittee",
        s."Name" as committee_name,
        s."Address1",
        s."Address2",
        s."City",
        s."State",
        s."Zip",
        s."PartyAffiliation",
        l."CandidateID",
        concat_ws(' ', c."FirstName", c."LastName") as candidate_name,
        row_number() over (
            partition by s."ID"
            order by c."LastName", c."FirstName"
        ) as rn
    from source_data s
    left join {{ source('il', 'il_cmte_candidate_links_landing') }} l
        on s."ID" = l."CommitteeID"
    left join {{ source('il', 'il_candidates_landing') }} c
        on l."CandidateID" = c."ID"
),

deduped as (
    select *
    from joined
    where rn = 1
),

transformed_data as (
    select
        'IL' as source,
        'IL_' || committee_id_raw as committee_id,
        case "TypeOfCommittee"
            when 'Candidate' then 'P'
            when 'Political Party' then 'U'
            when 'Political Action' then 'U'
            when 'Limited Activity Committee' then 'U'
            when 'Ballot Initiative' then 'U'
            when 'Independent Expenditure' then 'U'
        end as committee_designation,
        committee_name as name,              -- committee name
        "Address1" as address1,
        "Address2" as address2,
        "City" as city,
        "State" as state,
        "Zip" as zip_code,
        "PartyAffiliation" as affiliation,
        null as district,
        case when "CandidateID" is not null then 'IL_' || "CandidateID" end as candidate_id,
        candidate_name,                      -- candidate name
        current_timestamp as insert_datetime
    from deduped
)

select
    source,
    committee_id,
    candidate_id,
    candidate_name,   -- candidate name
    committee_designation,
    name,             -- committee name
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    insert_datetime
from transformed_data

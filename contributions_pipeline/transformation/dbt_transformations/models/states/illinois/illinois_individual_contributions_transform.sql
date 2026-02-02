{{
    config(
      materialized='table',
      tags=["illinois", "contributions", "master"]
    )
}}

-- Landing tables docs: https://elections.il.gov/campaigndisclosuredatafiles/CampaignDisclosureDataDictionary.txt

with source_data as (
    select
        "ID",
        "CommitteeID",
        coalesce(nullif("FirstName", '') || ' ', '') || "LastOnlyName" as contributor_name,
        "RcvDate",
        "Amount",
        "Occupation",
        "Employer",
        "City",
        "State",
        "Zip"
    from {{ source('il', 'il_receipts_landing') }}
    where "D2Part" = '1A'   -- Individual contributions only
),



joined as (
    select
        s.*,
        l."CandidateID",
        concat_ws(' ', c."FirstName", c."LastName") as candidate_name,
        row_number() over (
            partition by s."ID"
            order by c."LastName", c."FirstName"
        ) as rn
    from source_data s
    left join {{ source('il', 'il_cmte_candidate_links_landing') }} l
        on s."CommitteeID" = l."CommitteeID"
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
        'IL_' || "ID" as source_id,
        'IL_' || "CommitteeID" as committee_id,
        'IL_' || "CandidateID" as candidate_id,
        contributor_name as name,              -- contributor name
        candidate_name,                        -- candidate name from candidates table

        -- normalize city values that are only dots/whitespace to NULL
        nullif(
            btrim(regexp_replace("City", '\\.', '', 'g')),
            ''
        ) as city,

        "State" as state,
        "Zip" as zip_code,
        "Employer" as employer,
        "Occupation" as occupation,
        "Amount"::decimal as amount,
        to_timestamp("RcvDate", 'YYYY-MM-DD HH24:MI:SS') as contribution_datetime,
        current_timestamp as insert_datetime
    from deduped
)

select
    source,
    source_id,
    committee_id,
    candidate_id,
    name,             -- contributor name
    candidate_name,   -- candidate name
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
from transformed_data

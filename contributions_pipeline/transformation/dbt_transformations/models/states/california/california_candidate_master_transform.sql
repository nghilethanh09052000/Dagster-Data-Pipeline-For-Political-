{{
    config(
      materialized='table',
      tags=["california", "candidates", "master"]
    )
}}

with source_data as (
    select
        cand_naml,
        cand_namf
    from {{ source('ca', 'ca_cvr_campaign_disclosure') }}
    where
        cand_naml is not null
        and trim(cand_naml) <> ''
        -- Cal-Access docs says this is "Candidate/Office-holder
        -- (F460, F465, F470, F496, F497, F470S)" instead of the
        -- other types which are Committees
        and entity_cd = 'CAO'
    group by
        cand_naml,
        cand_namf
),

transformed_data as (
    select
        'CA' as "source",
        -- Need to crate surrogate key as there is cand_id
        -- on ca_cvr_campaign_disclosure, but literally only
        -- one record have it
        'CA_' || {{ dbt_utils.generate_surrogate_key(['cand_namf', 'cand_naml']) }} as candidate_id,
        coalesce(nullif(cand_namf, '') || ' ', '') || cand_naml as "name",
        null as affiliation,
        current_timestamp as insert_datetime
    from source_data
)

select
    source,
    candidate_id,
    name,
    affiliation,
    insert_datetime
from transformed_data

{{
    config(
        materialized='table',
        tags=["idaho", "committee", "master"]
    )
}}

with source_data as (
    select
        'ID' as source,
        'ID_OLD_COMM_' || {{ dbt_utils.generate_surrogate_key(['committee_name', 'party']) }} as committee_id,
        null as committee_designation,
        committee_name as "name",
        null as address1,
        null as address2,
        null as city,
        null as "state",
        null as zip_code,
        party as affiliation,
        null as district
    from {{ source('id', 'id_old_committee_contributions_landing') }}
    where committee_name <> ''
    group by
        committee_name,
        party

    union all

    select
        'ID' as source,
        'ID_OLD_CAN_' || {{ dbt_utils.generate_surrogate_key(['cand_last', 'cand_first', 'cand_middle', 'cand_suffix', 'party', 'district', 'office']) }} as committee_id,
        null as committee_designation,
        coalesce(nullif(cand_first, '') || ' ', '')
        || coalesce(nullif(cand_middle, '') || ' ', '')
        || cand_last
        || case when cand_suffix is not null or trim(cand_suffix) != '' then cand_suffix else '' end as "name",
        null as address1,
        null as address2,
        null as city,
        null as "state",
        null as zip_code,
        party as affiliation,
        district as district
    from {{ source('id', 'id_old_candidate_contributions_landing') }}
    where cand_last <> ''
    group by
        cand_last,
        cand_first,
        cand_middle,
        cand_suffix,
        party,
        district,
        office

    union all

    select
        'ID' as source,
        'ID_NEW_' || filing_entity_id as committee_id,
        null as committee_designation,
        MAX(filing_entity_name) as "name",
        null as address1,
        null as address2,
        null as city,
        null as "state",
        null as zip_code,
        null as affiliation,
        null as district
    from {{ source('id', 'id_new_contributions_landing') }}
    group by
        filing_entity_id
)

select
    "source",
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    current_timestamp as insert_datetime
from source_data

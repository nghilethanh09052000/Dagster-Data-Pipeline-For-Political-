{{
    config(
      materialized='table',
      tags=["master_tables"]
    )
}}

{%- set states_with_cand_id = [
    'federal',
    'california',
    'texas',
    'virginia',
    'nevada',
    'ohio',
    'washington',
    'nebraska',
    'new_mexico',
    'iowa',
    'illinois',
    'new_hampshire',
    'colorado',
    'vermont',
    'massachussets',
] %}

{%- set states = [
    'alabama',
    'alaska',
    'arkansas',
    'delaware',
    'montana',
    'new_york',
    'mississippi',
    'louisiana',
    'new_jersey',
    'florida',
    'michigan',
    'north_dakota',
    'connecticut',
    'kentucky',
    'south_carolina',
    'kansas',
    'tennessee',
    'oklahoma',
    'maryland',
    'wisconsin',
    'georgia',
    'district_of_columbia',
    'maine',
    'hawaii',
    'arizona',
    'rhode_island',
    'oregon',
    'wyoming',
    'idaho',
    'north_carolina',
    'indiana',
    'utah',
    'missouri',
    'west_virginia',
    'pennsylvania',
    'minnesota',
] %}

with source_data as (
    {%- for state in states_with_cand_id %}
        select
            source,
            committee_id,
            candidate_id,
            committee_designation,
            name,
            -- address1,
            -- address2,
            -- city,
            -- state,
            -- zip_code,
            affiliation,
            district
            -- ,
            -- insert_datetime
        from {{ ref(state ~ '_committee_master_transform') }}
        where source is not null and committee_id is not null and name is not null
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}

    union all

    {%- for state in states %}
        select
            source,
            committee_id,
            null as candidate_id,
            committee_designation,
            name,
            -- address1,
            -- address2,
            -- city,
            -- state,
            -- zip_code,
            affiliation,
            district
            -- ,
            -- insert_datetime
        from {{ ref(state ~ '_committee_master_transform') }}
        where source is not null and committee_id is not null and name is not null
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}
),

clean_deduped_data as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by committee_id
            ) as row_num
        from source_data
    ) as source_with_row_num
    where row_num = 1
)

select clean_deduped_data.* from clean_deduped_data
-- Only include rows that correspond to a committee_id in the individual contributions table:
where exists
(
    SELECT 1
    FROM individual_contributions
    WHERE clean_deduped_data.committee_id = individual_contributions.committee_id
)

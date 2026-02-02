{{ 
    config(
      materialized='table',
      tags=["master_tables"]
    )
}}

{%- set states_with_candidate = [
    'federal',
    'california',
    'texas',
    'virginia',
    'nevada',
    'ohio',
    'washington',
    'west_virginia',
    'montana',
    'new_mexico',
    'nebraska',
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
    'arizona',
    'arkansas',
    'delaware',
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
    'texas',
    'oklahoma',
    'maryland',
    'wisconsin',
    'rhode_island',
    'georgia',
    'district_of_columbia',
    'maine',
    'hawaii',
    'oregon',
    'wyoming',
    'idaho',
    'north_carolina',
    'indiana',
    'utah',
    'missouri',
    'pennsylvania',
    'minnesota',
] %}

with source_data as (
    {%- for state in states_with_candidate %}
        select
            ic.source,
            ic.source_id,
            ic.committee_id,
            ic.candidate_id,
            ic.candidate_name,
            -- name,
            -- city,
            -- state,
            -- zip_code,
            UPPER(SPLIT_PART(ic."name", ' ', 1) || '|' || SPLIT_PART(ic."name", ' ', -1) || '|' || LEFT(COALESCE(ic.zip_code, ''), 5)) as name_concat,
            CAST(ic.employer AS VARCHAR(20)) as employer,
            CAST(ic.occupation AS VARCHAR(20)) as occupation,
            ic.amount::int as amount,
            ic.contribution_datetime::date as contribution_datetime,
            cm.name::text as committee_name
            -- insert_datetime
        from {{ ref(state ~ '_individual_contributions_transform') }} ic
        left join {{ ref('committee_master') }} cm on ic.committee_id = cm.committee_id
        left join {{ ref('candidate_master') }} cam on ic.candidate_id = cam.candidate_id
        where 1=1
            and ic.contribution_datetime >= '2015-01-01'
            and ic.source is not null
            and ic.source_id is not null
            and ic.committee_id is not null
            and ic."name" is not null
            and ic.amount is not null
            -- Safety filter to filter out all out of int range contributions amount
            and ic.amount < 2147483647
            and ic.amount > -2147483648
            and ic.contribution_datetime is not null
            -- and insert_datetime is not null
            and ic.zip_code is not null
            and ic."name" <> ''
            and ic.zip_code <> ''
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}

    union all

    {%- for state in states %}
        select
            ic.source,
            ic.source_id,
            ic.committee_id,
            null as candidate_id,
            null as candidate_name,
            -- name,
            -- city,
            -- state,
            -- zip_code,
            UPPER(SPLIT_PART(ic."name", ' ', 1) || '|' || SPLIT_PART(ic."name", ' ', -1) || '|' || LEFT(COALESCE(ic.zip_code, ''), 5)) as name_concat,
            CAST(ic.employer AS VARCHAR(20)) as employer,
            CAST(ic.occupation AS VARCHAR(20)) as occupation,
            ic.amount::int as amount,
            ic.contribution_datetime::date as contribution_datetime,
            cm.name::text as committee_name
            -- insert_datetime
        from {{ ref(state ~ '_individual_contributions_transform') }} ic
        left join {{ ref('committee_master') }} cm on ic.committee_id = cm.committee_id
        where 1=1
            and ic.contribution_datetime >= '2015-01-01'
            and ic.source is not null
            and ic.source_id is not null
            and ic.committee_id is not null
            and ic."name" is not null
            and ic.amount is not null
            -- Safety filter to filter out all out of int range contributions amount
            and ic.amount < 2147483647
            and ic.amount > -2147483648
            and ic.contribution_datetime is not null
            -- and insert_datetime is not null
            and ic.zip_code is not null
            and ic."name" <> ''
            and ic.zip_code <> ''
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}
),

clean_dedup_data as (
    select
            source,
            committee_id,
            source_id,
            name_concat,
            employer,
            occupation,
            amount,
            contribution_datetime,
            committee_name,
            candidate_id,
            candidate_name,
            null::text as candidate_office
    from (
        select
            source_data.*,
            row_number() over (partition by source_id) as row_num
        from source_data
    ) as source_with_row_num
    where row_num = 1
)

select * from clean_dedup_data

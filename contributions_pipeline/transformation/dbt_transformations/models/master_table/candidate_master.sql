{{
    config(
      materialized='table',
      tags=["master_tables"]
    )
}}

{%- set states = [
    'federal',
    'california',
    'texas',
    'virginia',
    'nevada',
    'ohio',
    'washington',
    'arkansas',
    'alabama',
    'new_jersey',
    'alaska',
    'delaware',
    'west_virginia',
    'montana',
    'new_mexico',
    'kentucky',
    'nebraska',
    'iowa',
    'illinois',
    'new_hampshire',
    'colorado',
    'vermont',
    'massachussets'
] %}

with source as (
    {%- for state in states %}
        select
            source,
            candidate_id,
            name,
            affiliation,
            insert_datetime
        from {{ ref(state ~ '_candidate_master_transform') }}
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}
)

select
    source,
    candidate_id,
    name,
    affiliation,
    insert_datetime
from source

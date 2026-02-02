{{
    config(
      materialized='table',
      tags=["federal", "candidate"]
    )
}}

with source_data as (
    select
        cand_id,
        cand_name,
        cand_pty_affiliation
    from {{ source('fec', 'federal_candidate_master_landing') }}
),

transformed_data as (
    select
        'FEC' as "source",
        'FEC_' || cand_id as candidate_id,
        case
            when cand_name like '%,%'
                then
                    trim(split_part(cand_name, ',', 2))
                    || ' '
                    || trim(split_part(cand_name, ',', 1))
            else cand_name
        end as "name",
        cand_pty_affiliation as affiliation,
        current_timestamp as insert_datetime,
        row_number() over (
            partition by cand_id
        ) as row_num
    from source_data
)

select
    "source",
    candidate_id,
    "name",
    affiliation,
    insert_datetime
from transformed_data
where row_num = 1

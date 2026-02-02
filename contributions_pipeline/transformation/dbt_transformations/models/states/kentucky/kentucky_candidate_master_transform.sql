{{
    config(
      materialized='table',
      tags=["kentucky", "candidates", "master"]
    )
}}

with base as (

    select
        'KY' as source,

        concat(
            'KY_',
            {{ dbt_utils.generate_surrogate_key([
                'last_name',
                'first_name'
            ]) }}
        ) as candidate_id,

        concat_ws(' ', first_name, last_name) as name,

        null as affiliation,

        current_timestamp at time zone 'utc' as insert_datetime,

        -- dedupe helper
        row_number() over (
            partition by 
                last_name,
                first_name
            order by 
                election_date asc  -- keep earliest election_date if multiple exist
        ) as row_num

    from {{ source('ky', 'ky_candidates_landing') }}

),

deduplicated as (

    select *
    from base
    where row_num = 1

)

select
    source,
    candidate_id,
    name,
    affiliation,
    insert_datetime
from deduplicated

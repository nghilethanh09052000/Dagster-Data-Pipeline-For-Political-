{{
    config(
        materialized='table',
        tags=["indiana", "committee", "master"]
    )
}}

with committee_data as (
    select 
        distinct regexp_replace(initcap(lower(comm_name)), '[^a-zA-Z ]', '', 'g') as name,
        initcap(lower(street_address)) as address1,         
        initcap(lower(city)) as city, 
        state, 
        zip_code, 
        case when party = '' then null else party end as affiliation
    from {{ source('in', 'in_committee_data_landing_table') }}
    where 
        comm_name <> '' and
        street_address <> '' and
        city <> '' and
        state <> '' and
        zip_code <> ''
),
cleaned_data as (
    select
		*
	from (
		select
			*,
			row_number() over (partition by name order by address1 desc) as rn --rank contribution by amount
		from committee_data
	) as temp
	where rn = 1
)
select 
    'IN' as source,
    {{ generate_committee_id('IN_', 'name') }} AS committee_id,
    NULL as committee_designation,
    name,
    address1,
    NULL as address2,
    city,
    state,
    zip_code,
    affiliation,
    NULL as district,
    CURRENT_TIMESTAMP as insert_datetime
from cleaned_data




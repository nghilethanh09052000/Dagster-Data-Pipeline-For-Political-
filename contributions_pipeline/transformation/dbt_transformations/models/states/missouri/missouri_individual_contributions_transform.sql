{{
    config(
        materialized='table',
        tags=["missouri", "contributions", "master"]
    )
}}

with source_data as (
    select
        cd1_a."MECID" as committee_id,
        initcap(lower(cd1_a."Committee Name")) as committee_name,
        cd1_a."First Name" || ' ' || cd1_a."Last Name" as name,
        cd1_a."City" as city,
        cd1_a."State" as state,
        cd1_a."Zip" as zip_code,
        cd1_a."Employer" as employer,
        cd1_a."Occupation" as occupation,
        cd1_a."Date"::timestamp as contribution_datetime,
        sum(cd1_a."Amount"::decimal(10,2) + total_anonymous + total_inkind + total_monetary) as amount
    from {{ source('mo', 'mo_cd1_a_landing_table') }} as cd1_a
    join (
        select
            "MECID" as committee_id,
            case when "Total Anonymous" = '' then 0 else "Total Anonymous"::decimal(10,2) end as total_anonymous,
            case when "Total In-Kind" = '' then 0 else "Total In-Kind"::decimal(10,2) end as total_inkind,
            case when "Total Monetary" = '' then 0 else "Total Monetary"::decimal(10,2) end as total_monetary
        from {{ source('mo', 'mo_cd1_b_landing_table') }}
    ) as cd1_b on cd1_b.committee_id = cd1_a."MECID"
    where 
        "Zip" <> '' and
        "First Name" <> '' and
        "Last Name" <> '' and 
        "Amount" <> '' and
        "Date" <> ''
    group by 1,2,3,4,5,6,7,8,9
),
cleaned_data as (
    select
		*
	from (
		select
			*,
			row_number() over (partition by committee_id, name, zip_code order by amount desc) as rn --rank contribution by amount
		from source_data
	) as temp
	where rn = 1
)
select
    'MO' as source,
	'MO_' || {{ dbt_utils.generate_surrogate_key([
		'committee_id', 
		'name', 
		'zip_code', 
		'occupation',
		'contribution_datetime', 
		'amount'
	]) }} as source_id,
	'MO_' || committee_id as committee_id,
	name,
	city,
	state,
	zip_code,
	employer,
	occupation,
	amount,
	contribution_datetime,
	CURRENT_TIMESTAMP as insert_datetime
from cleaned_data

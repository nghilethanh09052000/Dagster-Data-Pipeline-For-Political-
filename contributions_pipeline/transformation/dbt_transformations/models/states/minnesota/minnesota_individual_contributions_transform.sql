{{
    config(
        materialized='table',
        tags=["minnesota", "contributions", "master"]
    )
}}

with source_data as (
    select
        "Recipient reg num" as committee_id,
        "Contributor" as name,
        null as city,
        null as state,
        "Contrib zip" as zip_code,
        "Contrib Employer name" as employer,
        null as occupation,
        "Receipt date"::timestamp as contribution_datetime,
        sum("Amount"::decimal(10,2)) as amount
    from {{ source('mn', 'mn_contrib_landing_table') }}
    where 
        "Contrib zip" <> '' and
        "Contributor" <> '' and
        "Amount" <> '' and
        "Receipt date" <> '' and
        "Contrib type" = 'Individual' and
        "Receipt type" = 'Contribution'
    group by 1,2,3,4,5,6,7,8
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
    'MN' as source,
	'MN_' || {{ dbt_utils.generate_surrogate_key([
		'committee_id', 
		'name', 
		'zip_code', 
		'contribution_datetime', 
		'amount'
	]) }} as source_id,
	'MN_' || committee_id as committee_id,
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

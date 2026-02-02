{{
    config(
      materialized='table',
      tags=["indiana", "contributions", "master"]
    )
}}

with contrib_data as (
    select
        {{ generate_committee_id('IN_', '"Committee"') }} as committee_id,
        regexp_replace(initcap(lower("Name")), '[^a-zA-Z ]', '', 'g') as "name",
        initcap(lower("City")) as city,
        "State" as "state",
        null as employer,
        "Zip" as zip_code,
        case
            when "Occupation" = '' then null
            else "Occupation"
        end as occupation,
        max("ContributionDate"::timestamp) as contribution_datetime,
        sum("Amount"::decimal(10, 2)) as amount
    from {{ source('in', 'in_contribution_data_landing_table') }}
    where
        "ContributorType" = 'Individual'
        and "Name" <> ''
        and "Zip" <> ''
        and "Amount" <> ''
    group by 1, 2, 3, 4, 5, 6, 7
),

cleaned_data as (
    select *
    from (
        select
            *,
            --rank contribution by amount
            row_number() over (
                partition by
                    committee_id,
                    "name",
                    "state",
                    zip_code,
                    occupation,
                    contribution_datetime,
                    amount
                order by amount desc
            ) as rn
        from contrib_data
    ) as "temp"
    where rn = 1
)

select
    'IN' as "source",
    'IN_' || {{ dbt_utils.generate_surrogate_key([
        'committee_id',
        'name',
        'state',
        'zip_code',
        'occupation',
        'contribution_datetime',
        'amount'
    ]) }} as source_id,
    committee_id,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    current_timestamp as insert_datetime
from cleaned_data

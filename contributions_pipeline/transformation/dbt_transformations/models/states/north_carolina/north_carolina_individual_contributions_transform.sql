{{ config(
    materialized='table',
    tags=["north_carolina", "contributions", "master"]
) }}

with source as (
    select distinct
        "CommitteeSBoEID" as raw_committee_id,
        "Name" as "name",
        "City" as city,
        "State" as "state",
        "ZipCode" as zip_code,
        "ProfessionOrJobTitle" as occupation,
        "EmployersNameOrSpecificField" as employer,
        "Amount" as amount,
        to_timestamp("DateOccured", 'MM/DD/YYYY') as contribution_datetime
    from {{ source('nc', 'nc_transactions_landing') }}
    where
        "TransctionType" = 'Individual'
        and "Amount" <> '' and "Amount" is not null
        and "Name" <> '' and "Name" is not null
        and "Name" <> 'Aggregated Individual Contribution'
),

transformed as (
    select
        'NC' as "source",
        'NC_' || {{ dbt_utils.generate_surrogate_key(
            [
                'name',
                'city',
                'state',
                'zip_code',
                'occupation',
                'employer',
                'amount',
                'contribution_datetime'
            ]
        ) }} as source_id,
        'NC_' || raw_committee_id as committee_id,
        name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        amount::numeric(10, 2) as amount,
        contribution_datetime,
        current_timestamp as insert_datetime
    from source
),

numbered as (
    select
        *,
        row_number() over (partition by source_id) as row_num
    from transformed
)

select
    "source",
    source_id,
    committee_id,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
from numbered
where row_num = 1

{{
    config(
        materialized='table',
        tags=["hawaii", "individual_contributions", "transform"]
    )
}}

with source as (

    select distinct
        "Noncandidate Committee Name" as committee_name,
        "Contributor Name" as contributor_name,
        "Date" as contribution_date,
        "Amount" as amount,
        "Aggregate" as aggregate,
        "Employer" as employer,
        "Occupation" as occupation,
        "Address 1" as address_1,
        "Address 2" as address_2,
        "City" as city,
        "State" as state,
        "Zip Code" as zip_code,
        "Non-Monetary (Yes or No)" as non_monetary,
        "Non-Monetary Category" as non_monetary_category,
        "Non-Monetary Description" as non_monetary_description,
        "Reg No" as reg_no,
        "Election Period" as election_period,
        "Mapping Location" as mapping_location
    from {{ source('hi', 'hi_contrib_recv_by_non_candidates_landing_table') }}
    where "Noncandidate Committee Name" is not null and "Noncandidate Committee Name" <> ''
      and "Amount" is not null and "Amount" <> ''
      and "Date" is not null
      and "Reg No" is not null
      and "Contributor Name" <> 'Contributor Name' -- in case of header rows

),

transformed as (

    select
        'HI' as source,
        'HI_' || {{ dbt_utils.generate_surrogate_key([
            'committee_name',
            'contributor_name',
            'contribution_date',
            'amount',
            'aggregate',
            'employer',
            'occupation',
            'address_1',
            'address_2',
            'city',
            'state',
            'zip_code',
            'non_monetary',
            'non_monetary_category',
            'non_monetary_description',
            'reg_no',
            'election_period',
            'mapping_location'
        ]) }} as source_id,
        {{ generate_committee_id('HI_', 'committee_name') }} as committee_id,
        contributor_name as name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        amount::numeric(10,2) as amount,
        cast(contribution_date as timestamp) as contribution_datetime,
        current_timestamp at time zone 'UTC' as insert_datetime

    from source

)

select *
from transformed

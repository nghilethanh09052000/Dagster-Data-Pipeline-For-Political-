{{
    config(
        materialized = 'table',
        tags = ["massachussets", "contributions", "master"]
    )
}}

with base as (
    select
        'MA' as source,

        'MA_' || {{ dbt_utils.generate_surrogate_key([
            'record_type_id',
            'filer_cpf_id',
            'date',
            'name',
            'first_name',
            'address',
            'zip_code',
            'city',
            'state',
            'employer',
            'occupation',
            'amount'
        ]) }} as source_id,

        'MA_' || nullif(filer_cpf_id, '') as committee_id,

        regexp_replace(
            coalesce(nullif(first_name, ''), '') || ' ' || coalesce(nullif(name, ''), ''),
            '[^a-zA-Z ]',
            '',
            'g'
        ) as name,

        nullif(city, '') as city,
        nullif(state, '') as state,
        nullif(zip_code, '') as zip_code,
        nullif(employer, '') as employer,
        nullif(occupation, '') as occupation,

        nullif(amount, '')::decimal(10,2) as amount,
        to_timestamp(nullif(date, ''), 'MM/DD/YYYY') as contribution_datetime,
        current_timestamp as insert_datetime

    from {{ source('ma', 'ma_contributions_landing') }}
    where
        nullif(amount, '') is not null
        and nullif(date, '') is not null
        and nullif(filer_cpf_id, '') is not null
        and record_type_description = 'Individual'
)

, candidate_data as (
    select
        'MA_' || "CPF ID" as committee_id,
        case
            when "Candidate First Name" <> ''  then 'MA_' || "CPF ID"
            else null
        end as candidate_id,
        trim(nullif("Candidate First Name", '')) || ' ' || trim(nullif("Candidate Last Name", '')) as candidate_name
    from {{ source('ma', 'ma_committee_candidate_landing_table') }}
)

select distinct
    b.source,
    b.source_id,
    b.committee_id,
    c.candidate_id,
    c.candidate_name,
    b.name,
    b.city,
    b.state,
    b.zip_code,
    b.employer,
    b.occupation,
    b.amount,
    b.contribution_datetime,
    b.insert_datetime
from base b
left join candidate_data c
    on b.committee_id = c.committee_id

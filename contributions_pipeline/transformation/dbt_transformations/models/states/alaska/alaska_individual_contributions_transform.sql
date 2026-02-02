{{ config(
    materialized='table',
    tags=["alaska", "individual_contributions", "combined"]
) }}


with source_contribution as (
    select
        filer_name,
        contact_first_name,
        contact_last_name,
        occupation,
        employer,
        total_contributed_this_report,
        submitted,
        filer_city,
        filer_state,
        filer_zip
    from
        {{ source('ak', 'ak_contribution_reports_landing') }}
    where
        filer_type = 'Other'
        and total_contributed_this_report is not null
        and total_contributed_this_report <> ''
        and filer_name is not null and filer_name <> ''
),

transformed as (
    select
        'AK' as "source",
        'AK_' || {{ dbt_utils.generate_surrogate_key(
            [
                'filer_name',
                'contact_first_name',
                'contact_last_name',
                'occupation',
                'employer',
                'total_contributed_this_report',
                'submitted'
        ]) }} as source_id,
        {{ generate_committee_id('AK_', 'filer_name') }} as committee_id,
        trim(contact_first_name || ' ' || contact_last_name) as name,
        {{ clean_dolar_sign_amount('total_contributed_this_report') }} as amount,
        filer_city as city,
        filer_state as "state",
        filer_zip as zip_code,
        employer,
        occupation,
        cast(submitted as timestamp) as contribution_datetime,
        current_timestamp as insert_datetime
    from source_contribution
),

final as (
    select
        *,
        row_number() over (
            partition by source_id
        ) as row_num
    from transformed
)

select *
from final
where row_num = 1

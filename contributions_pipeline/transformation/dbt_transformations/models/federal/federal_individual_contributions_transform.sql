{{
    config(
      materialized='incremental',
      unique_key='source_id',
      cluster_by=['partition_date'],
      incremental_strategy='delete+insert',
      incremental_predicate=["DBT_INTERNAL_DEST.partition_date >= min(DBT_INTERNAL_SOURCE.partition_date)"],
      tags=["federal", "contributions"]
    )
}}

with source_earmarked_transformed as (
    select
        case
            when
                ic.memo_text_description ~ 'C\d{8}'
                -- Get the first matches
                then
                    ((regexp_match(ic.memo_text_description, 'C\d{8}'))[1])
            else ic.filer_committee_id_number
        end as transformed_committee_id,
        ic.partition_date,
        ic.contributor_name,
        ic.contributor_last_name,
        ic.contributor_first_name,
        ic.contributor_middle_name,
        ic.contributor_city,
        ic.contributor_state,
        ic.contributor_zip_code,
        ic.contributor_employer,
        ic.contributor_occupation,
        ic.contribution_date,
        ic.contribution_amount,
        ic.memo_text_description
    from {{ source('fec', 'federal_schedule_a_landing') }} as ic
    where
        (
            -- Form 3 and 3X individual contributions
            lower(ic.form_type) = 'sa11a'
            or lower(ic.form_type) = 'sa11ai'
            -- Form 3P individual contributions
            or lower(ic.form_type) = 'sa17a'
        )
        and (
            -- For contributions newer than v2
            ic.entity_type = 'IND'
            -- For contributions >v2
            or ic.entity_type = ''
            or ic.entity_type is null
        )
        -- Only takes in valid rows with datetime format 'MMDDYYYY'
        and char_length(ic.contribution_date) = 8
        and ic.filer_committee_id_number is not null
        and trim(ic.filer_committee_id_number) != ''

        {% if is_incremental() %}

            and ic.partition_date
            >= (
            -- coalesce partition time is set to the earliest possible partition
            -- WARN: make sure to match this to the
            -- FEC_FIRST_DATE_ELECTRONIC_FILED_REPORTS_BULK_DATA_AVAILABLE
            -- variable in FEC asset!
                select coalesce(max(prev.partition_date), '20150101')
                from {{ this }} as prev
            )

        {% endif %}
),

clean_source as (
    select
        cm.committee_id,
        cm.candidate_id,
        cm.candidate_name,
        ic.partition_date,
        ic.contributor_name,
        ic.contributor_last_name,
        ic.contributor_first_name,
        ic.contributor_middle_name,
        ic.contributor_city,
        ic.contributor_state,
        ic.contributor_zip_code,
        ic.contributor_employer,
        ic.contributor_occupation,
        ic.contribution_date,
        ic.contribution_amount,
        ic.memo_text_description
    from source_earmarked_transformed as ic
    left join {{ ref('federal_committee_master_transform') }} as cm
        on ic.transformed_committee_id = cm.cmte_id
),

numbered as (
    select
        *,
        row_number()
            over (
                partition by partition_date
                order by contribution_date
            )
            as sub_id
    from clean_source
),

transformed as (
    select
        'FEC' as "source",
        partition_date,
        ('FEC_' || partition_date || '/' || sub_id) as source_id,
        committee_id,
        candidate_id,
        candidate_name,
        case
            when contributor_name is not null and contributor_name like '%,%'
                then
                    trim(split_part(contributor_name, ',', 2))
                    || ' '
                    || trim(split_part(contributor_name, ',', 1))
            else trim(
                coalesce(nullif(contributor_first_name, '') || ' ', '')
                || coalesce(nullif(contributor_middle_name, '') || ' ', '')
                || contributor_last_name
            )
        end as "name",
        contributor_city as city,
        contributor_state as "state",
        contributor_zip_code as zip_code,
        contributor_employer as employer,
        contributor_occupation as occupation,
        contribution_amount::decimal as amount,
        (

            to_timestamp(
                contribution_date, 'YYYYMMDD'
            )::timestamp
        )
            as contribution_datetime,
        current_timestamp as insert_datetime
    from numbered
)

select * from transformed

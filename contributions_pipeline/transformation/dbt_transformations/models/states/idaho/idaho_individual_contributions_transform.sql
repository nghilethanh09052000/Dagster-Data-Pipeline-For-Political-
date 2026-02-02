{{
    config(
        materialized='table',
        tags=["idaho", "individual_contributions", "transform"]
    )
}}

with source_data as (
    select
        'ID' as source,
        'ID_OLD_COMM_' || {{ dbt_utils.generate_surrogate_key([
            'committee_name',
            'party',
            'contrib_first',
            'contrib_middle',
            'contrib_last',
            'contrib_suffix',
            'contrib_city',
            'contrib_state',
            'contrib_zip',
            'contrib_date',
            'contrib_amount',
        ]) }} as source_id,
        'ID_OLD_COMM_' || {{ dbt_utils.generate_surrogate_key(['committee_name', 'party']) }} as committee_id,
        coalesce(nullif(contrib_first, '') || ' ', '')
        || coalesce(nullif(contrib_middle, '') || ' ', '')
        || contrib_last
        || case when contrib_suffix is not null or trim(contrib_suffix) != '' then contrib_suffix else '' end as "name",
        contrib_city as city,
        contrib_state as "state",
        contrib_zip as zip_code,
        null as employer,
        null as occupation,
        contrib_amount::decimal as amount,
        {{ parse_flexible_date('contrib_date') }} as contribution_datetime,
        row_number() over (
            partition by
              committee_name,
              party,
              contrib_first,
              contrib_middle,
              contrib_last,
              contrib_suffix,
              contrib_city,
              contrib_state,
              contrib_zip,
              contrib_date,
              contrib_amount
        ) as row_num
    from {{ source('id', 'id_old_committee_contributions_landing') }}
    where
        contrib_date <> ''
        and contrib_last is not null
        and contrib_last <> ''

    union all

    select
        'ID' as source,
        'ID_OLD_CAN_' || {{ dbt_utils.generate_surrogate_key([
            'cand_last',
            'cand_first',
            'cand_middle',
            'cand_suffix',
            'party',
            'office',
            'district',
            'contrib_first',
            'contrib_middle',
            'contrib_last',
            'contrib_suffix',
            'contrib_city',
            'contrib_state',
            'contrib_zip',
            'contrib_date',
            'contrib_amount',
        ]) }} as source_id,
        'ID_OLD_CAN_' || {{ dbt_utils.generate_surrogate_key(['cand_last', 'cand_first', 'cand_middle', 'cand_suffix', 'party', 'district', 'office']) }} as committee_id,
        coalesce(nullif(contrib_first, '') || ' ', '')
        || coalesce(nullif(contrib_middle, '') || ' ', '')
        || contrib_last
        || case when contrib_suffix is not null or trim(contrib_suffix) != '' then contrib_suffix else '' end as "name",
        contrib_city as city,
        contrib_state as "state",
        contrib_zip as zip_code,
        null as employer,
        null as occupation,
        contrib_amount::decimal as amount,
        {{ parse_flexible_date('contrib_date') }} as contribution_datetime,
        row_number() over (
            partition by
              cand_last,
              cand_first,
              cand_middle,
              cand_suffix,
              party,
              office,
              district,
              contrib_first,
              contrib_middle,
              contrib_last,
              contrib_suffix,
              contrib_city,
              contrib_state,
              contrib_zip,
              contrib_date,
              contrib_amount
        ) as row_num
    from {{ source('id', 'id_old_candidate_contributions_landing') }}
    where
        contrib_date <> ''
        and contrib_last is not null
        and contrib_last <> ''

    union all

    select
        'ID' as source,
        'ID_NEW_' || transaction_id as source_id,
        'ID_NEW_' || filing_entity_id as committee_id,
        coalesce(nullif(contributor_first_name, '') || ' ', '')
        || contributor_last_name as "name",
        contributor_address_city as city,
        contributor_address_state as "state",
        contributor_address_zip_code as zip_code,
        null as employer,
        null as occupation,
        {{ clean_dolar_sign_amount('transaction_amount') }} as amount,
        to_timestamp(transaction_date, 'MM/DD/YYYY HH:MI:SS') as contribution_datetime,
        row_number() over (
            partition by transaction_id
        ) as row_num
    from {{ source('id', 'id_new_contributions_landing') }}
    where
        transaction_type = 'Contribution'
        and amended = 'N'
)

select
    "source",
    source_id,
    committee_id,
    "name",
    city,
    "state",
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    current_timestamp as insert_datetime
from source_data
where row_num = 1

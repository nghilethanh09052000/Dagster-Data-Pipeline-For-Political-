{{ config(
    materialized='table',
    tags=["california", "contributions", "master"]
) }}

with source as (

    select
        s.filing_id,
        s.tran_id,
        s.cmte_id,
        s.enty_naml,
        s.enty_namf,
        s.enty_namt,
        s.enty_nams,
        s.enty_city,
        s.enty_st,
        s.enty_zip4,
        s.ctrib_emp,
        s.ctrib_occ,
        s.ctrib_date,
        s.amount,
        cm.candidate_id,
        cm.candidate_name
    from {{ source('ca', 'ca_s497') }} s
    left join {{ ref('california_committee_master_transform') }} as cm
        on s.cmte_id = cm.filer_id
    where amount <> ''
),

transformed as (
    select
        'CA' as source,
        'CA_' || {{ dbt_utils.generate_surrogate_key([
            'filing_id',
            'tran_id',
            'cmte_id',
            'enty_naml',
            'enty_namf',
            'enty_namt',
            'enty_nams',
            'enty_city',
            'enty_st',
            'enty_zip4',
            'ctrib_emp',
            'ctrib_occ',
            'ctrib_date',
            'amount'
        ]) }} as source_id,
        'CA_' || cmte_id as committee_id,
        candidate_id,
        candidate_name,
        trim(concat_ws(' ', enty_namf, enty_namt, enty_naml, enty_nams))
            as name,
        enty_city as city,
        enty_st as state,
        enty_zip4 as zip_code,
        ctrib_emp as employer,
        ctrib_occ as occupation,
        amount::numeric as amount,
        to_timestamp(ctrib_date, 'MM/DD/YYYY HH12:MI:SS AM')
            as contribution_datetime,
        current_timestamp as insert_datetime
    from source
)

select distinct * from transformed

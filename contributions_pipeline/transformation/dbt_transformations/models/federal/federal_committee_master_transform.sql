{{
    config(
      materialized='table',
      indexes=[
        {'columns': ['cmte_id']},
      ],
      tags=["federal", "candidate"],
    )
}}

with source as (
    select
        cm.cmte_id,
        cm.cmte_nm,
        cm.tres_nm,
        cm.cmte_st1,
        cm.cmte_st2,
        cm.cmte_city,
        cm.cmte_st,
        cm.cmte_zip,
        cm.cmte_dsgn,
        cm.cmte_tp,
        cm.cmte_pty_affiliation,
        cm.cmte_filing_freq,
        cm.org_tp,
        cm.connected_org_nm,
        cam.candidate_id,
        cam.name as candidate_name
    from {{ source('fec', 'federal_committee_master_landing') }} as cm
    left join {{ ref('federal_candidate_master_transform') }} as cam
        on ('FEC_' || cm.cand_id) = cam.candidate_id
),

transformed as (
    select
        'FEC'::text as "source",
        ('FEC_' || cmte_id) as committee_id,
        cmte_id,
        candidate_id,
        candidate_name,
        org_tp as committee_designation,
        cmte_nm as "name",
        cmte_st1 as address1,
        cmte_st2 as address2,
        cmte_city as city,
        cmte_st as "state",
        cmte_zip as zip_code,
        cmte_pty_affiliation as affiliation,
        NULL as district,
        CURRENT_TIMESTAMP as insert_datetime,
        ROW_NUMBER() over (
            partition by cmte_id
        ) as row_num
    from source
)

select * from transformed
where row_num = 1

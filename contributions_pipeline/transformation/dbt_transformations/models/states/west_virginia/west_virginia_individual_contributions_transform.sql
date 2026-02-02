{{ config(
    materialized='table',
    tags=["west_virginia", "individual_contributions", "combined"]
) }}

with source as (

    select
        c.org_id,
        c.receipt_amount,
        c.receipt_date,
        c.first_name,
        c.middle_name,
        c.last_name,
        c.suffix,
        c.address1,
        c.address2,
        c.city,
        c.state,
        c.zip,
        c.receipt_id,
        c.employer,
        c.occupation,
        c.contribution_type,
        c.committee_name,
        c.candidate_name,
        -- Committee information for proper committee_id
        cmt."id_number" as committee_id_number,
        -- Candidate information for proper candidate_id
        cand."IDNumber" as candidate_id_number
    from {{ source('wv', 'wv_contributions_landing') }} c
    -- Left join with committees to get committee details
    left join {{ source('wv', 'wv_committees_landing') }} cmt
        on c.org_id = cmt."id_number"
    -- Left join with candidates to get candidate details
    left join {{ source('wv', 'wv_candidates_landing') }} cand
        on c.org_id = cand."IDNumber"
    where 
        c.receipt_amount is not null
        and c.receipt_amount <> ''

),

cleaned as (

    select
        'WV' as source,
        'WV_' || {{ dbt_utils.generate_surrogate_key([
            'org_id',
            'receipt_amount',
            'receipt_date',
            'first_name',
            'middle_name',
            'last_name',
            'suffix',
            'address1',
            'address2',
            'city',
            'state',
            'zip',
            'receipt_id',
            'employer',
            'occupation',
            'contribution_type',
            'committee_name',
            'candidate_name'
        ]) }} as source_id,
        'WV_' || COALESCE(committee_id_number, org_id) as committee_id,
        CASE 
            WHEN candidate_id_number IS NOT NULL THEN 'WV_' || candidate_id_number 
            ELSE NULL 
        END as candidate_id,
        nullif(trim(candidate_name), '') as candidate_name,
        trim(coalesce(first_name, '') || ' ' || coalesce(middle_name, '') || ' ' || coalesce(last_name, '') || ' ' || coalesce(suffix, '')) as name,
        nullif(trim(city), '') as city,
        nullif(trim(state), '') as state,
        coalesce(nullif(trim(zip), ''), '00000') as zip_code,
        nullif(trim(employer), '') as employer,
        nullif(trim(occupation), '') as occupation,
        CAST(NULLIF(regexp_replace(receipt_amount, '[^0-9\.]', '', 'g'), '') AS numeric) as amount,
        CAST(NULLIF(receipt_date, '') AS timestamp) as contribution_datetime,
        current_timestamp as insert_datetime
    from source

),

final as (

    select
        source,
        source_id,
        committee_id,
        candidate_id,
        candidate_name,
        name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        amount,
        contribution_datetime,
        insert_datetime
    from cleaned
)

select distinct * from final
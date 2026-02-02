{{
    config(
        materialized='table',
        tags=["iowa", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        date,
        committee_code,
        committee_type,
        committee_name,
        contributing_committee_code,
        first_name,
        last_name,
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code,
        contribution_amount,
        check_number
    FROM {{ source('ia', 'ia_campaign_contributions_received_landing') }}
    WHERE date IS NOT NULL 
      AND date <> ''
      AND date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
)

, cleaned AS (
    SELECT DISTINCT
        'IA' AS source,
        'IA_' || {{ dbt_utils.generate_surrogate_key(
            [
                'a.date',
                'a.committee_code',
                'a.committee_type',
                'a.committee_name',
                'a.contributing_committee_code',
                'a.first_name',
                'a.last_name',
                'a.address_line_1',
                'a.address_line_2',
                'a.city',
                'a.state',
                'a.zip_code',
                'a.contribution_amount',
                'a.check_number'
            ]) 
        }} AS source_id,
        'IA_' || a.committee_code AS committee_id,
        case 
            WHEN candidate_name IS NOT NULL then  'IA_' || b.committee_number
            else NULL
        end as candidate_id,
        b.candidate_name AS candidate_name,
        TRIM(
            COALESCE(NULLIF(first_name, ''), '') || 
            CASE 
                WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN ' ' 
                ELSE '' 
            END || 
            COALESCE(NULLIF(last_name, ''), '')
        ) AS name,
        city,
        state,
        zip_code,
        NULL AS employer,
        NULL AS occupation,
        REGEXP_REPLACE(
            contribution_amount,
            ',', '',
            'g'
        )::decimal(10,2) AS amount,
        CASE 
            WHEN date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_timestamp(date, 'MM/DD/YYYY')
            ELSE NULL
        END AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data a
    left join {{ source('ia', 'ia_registered_political_candidates_landing') }} b
    on a.committee_code = b.committee_number
    WHERE contribution_amount IS NOT NULL
)

SELECT * FROM cleaned

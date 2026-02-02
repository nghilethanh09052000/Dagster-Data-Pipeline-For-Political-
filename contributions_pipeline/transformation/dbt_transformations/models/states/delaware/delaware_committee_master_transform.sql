{{
    config(
        materialized='table',
        tags=["delaware", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        cf_id,
        committee_type,
        committee_name,
        office
    FROM {{ source('de', 'de_committees_landing') }}
    where 
        cf_id is not null and cf_id <> '' 
)

, cleaned_data AS (
    SELECT
        'DE' as source,
        'DE_' || cf_id as committee_id,
        CASE
            WHEN committee_type = 'Candidate Committee' THEN 'P'  
            WHEN committee_type = 'Political Committee' THEN 'U'  
            WHEN committee_type = 'Certification of Intention' THEN 'U'
            ELSE NULL
        END as committee_designation,

        committee_name as name,
        NULL as address1,
        NULL as address2,
        NULL as city,
        NULL as state,
        NULL as zip_code,
        NULL as affiliation,
        office as district,
        CURRENT_TIMESTAMP as insert_datetime
    FROM source_data
    WHERE cf_id IS NOT NULL
        AND cf_id <> '' 
)

, final AS (
    SELECT *, row_number() over ( partition by committee_id ) as row_num FROM cleaned_data
)

SELECT * FROM final where row_num = 1
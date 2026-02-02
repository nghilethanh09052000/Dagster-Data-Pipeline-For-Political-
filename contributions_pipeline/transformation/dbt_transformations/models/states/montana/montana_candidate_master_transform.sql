{{ 
  config(
    materialized='table',
    tags=["montana", "candidates", "master"]
  ) 
}}

WITH source_data AS (
    SELECT
        candidate_id,
        candidate_name,
        party_descr,
        candidate_address,
        office_title,
        res_county_descr,
        candidate_status_descr,
        created_date,
        home_phone,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('mt', 'mt_candidates_landing') }}
    WHERE candidate_name IS NOT NULL AND TRIM(candidate_name) <> ''
),
transformed_data AS (
    SELECT
        'MT' AS source,
        'MT_' || candidate_id AS candidate_id,
        candidate_name AS name,
        party_descr AS affiliation,
        candidate_address AS address,
        office_title AS office_sought,
        res_county_descr AS district,
        candidate_status_descr AS status,
        created_date AS registration_date,
        home_phone AS phone_number,
        insert_datetime
    FROM source_data
),
ranked_data AS (
    SELECT
        source,
        candidate_id,
        name,
        affiliation,
        address,
        office_sought,
        district,
        status,
        registration_date,
        phone_number,
        insert_datetime,
        ROW_NUMBER() OVER (PARTITION BY candidate_id) AS row_num
    FROM transformed_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    address,
    office_sought,
    district,
    status,
    registration_date,
    phone_number,
    insert_datetime
FROM ranked_data
WHERE row_num = 1

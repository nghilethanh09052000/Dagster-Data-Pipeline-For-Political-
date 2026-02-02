{{
    config(
      materialized='table',
      tags=["alabama", "committee", "master"]
    )
}}

WITH pac_committees AS (
    SELECT
        'AL' AS source,
        'AL_' || committee_id AS committee_id,
        committee_name AS name,
        CASE 
            WHEN address IS NOT NULL AND address <> '' 
            THEN SPLIT_PART(address, ',', 1)
            ELSE NULL 
        END AS address1,
        NULL AS address2,
        COALESCE(city, NULL) AS city,
        'AL' AS state,
        CASE 
            WHEN address IS NOT NULL AND address <> '' 
            THEN CASE 
                -- Look for 5-digit zip code pattern at the end
                WHEN address ~ '[0-9]{5}$' THEN REGEXP_REPLACE(address, '.*([0-9]{5})$', '\1')
                -- Look for 5-digit zip code followed by state abbreviation
                WHEN address ~ '[0-9]{5} [A-Z]{2}$' THEN REGEXP_REPLACE(address, '.*([0-9]{5}) [A-Z]{2}$', '\1')
                -- Look for 5-digit zip code in the middle or end
                WHEN address ~ '[0-9]{5}' THEN REGEXP_REPLACE(address, '.*([0-9]{5}).*', '\1')
                ELSE NULL
            END
            ELSE NULL 
        END AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CASE 
            WHEN committee_type = 'Political Action Committee' THEN 'B'
            ELSE NULL
        END AS committee_designation,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('al', 'al_political_action_committee_landing') }}
    WHERE 
        committee_id IS NOT NULL 
        AND committee_name IS NOT NULL 
        AND TRIM(committee_name) <> ''
),

deduplicated_committees AS (
    SELECT DISTINCT
        source,
        committee_id,
        name,
        address1,
        address2,
        city,
        state,
        zip_code,
        affiliation,
        district,
        committee_designation,
        insert_datetime
    FROM pac_committees
)

SELECT
    source,
    committee_id,
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    committee_designation,
    insert_datetime
FROM deduplicated_committees

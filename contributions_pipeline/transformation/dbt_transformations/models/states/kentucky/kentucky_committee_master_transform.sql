{{ 
    config(
        materialized='table',
        tags=["kentucky", "committee", "master"]
    ) 
}}

WITH deduped AS (
    SELECT
        organization_name,
        MIN(organization_type) AS organization_type,
        MIN(office_sought) AS office_sought,
        MIN(location) AS location,
        MIN(chair_address1) AS chair_address1,
        MIN(chair_address2) AS chair_address2,
        MIN(chair_city) AS chair_city,
        MIN(chair_state) AS chair_state,
        NULL AS zip_code,
        NULL AS affiliation
    FROM {{ source('ky', 'ky_organizations_landing') }}
    WHERE organization_name IS NOT NULL
    GROUP BY organization_name
),

transformed AS (
    SELECT
        'KY' AS source,
        {{ generate_committee_id('KY_', 'organization_name') }} AS committee_id,

        NULL AS committee_designation,
        organization_name AS name,

        chair_address1 AS address1,
        chair_address2 AS address2,
        chair_city AS city,
        chair_state AS state,
        zip_code,
        affiliation,
        location AS district, 

        CURRENT_TIMESTAMP AS insert_datetime

    FROM deduped
)

SELECT * FROM transformed

{{ 
    config(
      materialized='table',
      tags=["district_of_columbia", "committees", "master"]
    )
}}

WITH source_data AS (
    SELECT 
        'DC' AS source,
        {{ generate_committee_id('DC_', 'committee_name') }} AS committee_id,
        NULL AS committee_designation,
        MIN(committee_name) AS name, 
        MIN(office) AS address1,  
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS zip_code,
        MIN(party) AS affiliation, 
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('dc', 'dc_committees_landing_table') }}
    WHERE committee_name IS NOT NULL AND committee_name <> '' 
    GROUP BY {{ generate_committee_id('DC_', 'committee_name') }} 
)

SELECT * FROM source_data

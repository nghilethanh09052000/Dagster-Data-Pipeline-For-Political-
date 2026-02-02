{{ 
    config(
      materialized='table',
      tags=["arizona", "committees", "master"]
    )
}}

WITH deduped_committees AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY committee_name) AS rn
        FROM {{ source('az', 'az_committees_landing') }}
    ) a
    WHERE rn = 1
),

source_data AS (
    SELECT
        'AZ' AS source,
        {{ generate_committee_id('AZ_', 'committee_name') }} AS committee_id,
        NULL AS committee_designation,
        committee_name AS name,
        address1,
        address2,
        city,
        state,
        zipcode AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM deduped_committees
)

SELECT * FROM source_data

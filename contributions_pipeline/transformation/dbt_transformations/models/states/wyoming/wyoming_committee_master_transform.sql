{{ 
    config(
        materialized='table',
        tags=["wyoming", "committee", "master"]
    )
}}

WITH source AS (
    SELECT DISTINCT
        "Recipient Name",
        "Recipient Type"
    FROM {{ source('wy', 'wy_contributions_landing_table') }}
),

final AS (
    SELECT
        'WY' AS source,
        'WY_' || {{ dbt_utils.generate_surrogate_key(['"Recipient Name"', '"Recipient Type"']) }} AS committee_id,
        CASE
            WHEN "Recipient Type" = 'CANDIDATE COMMITTEE' THEN 'A'
            WHEN "Recipient Type" = 'CANDIDATE' THEN 'A'
        END AS committee_designation,
        "Recipient Name" AS name,
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source
)

SELECT * FROM final

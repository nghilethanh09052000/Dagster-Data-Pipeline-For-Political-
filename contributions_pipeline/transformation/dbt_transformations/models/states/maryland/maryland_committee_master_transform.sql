{{
    config(
      materialized='table',
      tags=["maryland", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "ReceivingCommittee"
    FROM {{ source('md', 'md_contributions_and_loans_landing') }}
    GROUP BY "ReceivingCommittee"
),

transformed_data AS (
    SELECT
        'MD' AS "source",
        'MD_' || {{ dbt_utils.generate_surrogate_key(['"ReceivingCommittee"']) }} AS committee_id,
        NULL AS committee_designation,
        "ReceivingCommittee" AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

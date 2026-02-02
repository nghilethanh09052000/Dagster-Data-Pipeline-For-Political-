{{ 
    config(
        materialized='table',
        tags=["montana", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        committee_id,
        committee_name,
        committee_address,
        mailing_address,
        city,
        state,
        zip5,
        zip4
    FROM {{ source('mt', 'mt_committees_landing') }}
),

cleaned_data AS (
    SELECT
        'MT' AS source,
        'MT_' || committee_id AS committee_id,
        NULL AS committee_designation,
        committee_name AS name,
        COALESCE(committee_address, mailing_address) AS address1,
        NULL AS address2,
        city,
        state,
        COALESCE(zip5, zip4) AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE
        committee_id IS NOT NULL AND committee_id <> ''
),

final AS (
    SELECT
        source,
        committee_id,
        committee_designation,
        name,
        address1,
        address2,
        city,
        state,
        zip_code,
        affiliation,
        district,
        insert_datetime
    FROM cleaned_data
)

SELECT * FROM final


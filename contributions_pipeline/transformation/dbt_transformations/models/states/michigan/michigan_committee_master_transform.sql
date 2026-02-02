{{
    config(
      materialized='table',
      tags=["michigan", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        cfr_com_id,
        com_legal_name
    FROM {{ source('mi', 'mi_contributions_landing') }}
    GROUP BY
        cfr_com_id,
        com_legal_name
),

mitn_source_data AS (
    SELECT
        committee_id,
        committee_name
    FROM {{ source('mi', 'mi_mitn_contributions_landing') }}
    GROUP BY
        committee_id,
        committee_name
),

transformed_data AS (
    SELECT
        'MI' AS "source",
        'MI_' || cfr_com_id AS committee_id,
        NULL AS committee_designation,
        com_legal_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data

    UNION ALL

    SELECT
        'MI' AS "source",
        'MI_' || committee_id AS committee_id,
        NULL AS committee_designation,
        committee_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM mitn_source_data
),

numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY committee_id) AS row_num
    FROM transformed_data
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
FROM numbered
WHERE row_num = 1

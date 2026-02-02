{{ config(
    materialized='table',
    tags=['ohio', 'committee_master']
) }}

WITH candidate_active_source AS (
    SELECT
        "MASTER_KEY",
        trim(
            coalesce("CANDIDATE_FIRST_NAME", '')
            || ' '
            || coalesce("CANDIDATE_LAST_NAME", '')
        ) AS candidate_name,
        "PARTY" AS candidate_party,
        "COM_NAME",
        "COM_ADDRESS",
        "COM_CITY",
        "COM_STATE",
        "COM_ZIP",
        "PARTY",
        "DISTRICT"
    FROM {{ source('oh', 'oh_candidate_list_landing') }}
),

candidate_inactive_source AS (
    SELECT DISTINCT
        "MASTER_KEY",
        trim(
            coalesce("CANDIDATE_FIRST_NAME", '')
            || ' '
            || coalesce("CANDIDATE_LAST_NAME", '')
        ) AS candidate_name,
        "PARTY" AS candidate_party,
        "COM_NAME",
        "PARTY",
        "DISTRICT"
    FROM {{ source('oh', 'oh_candidate_contribution_landing') }}
),

candidate_transformed AS (
    SELECT
        'OH' AS "source",
        'OH_CAN_' || "MASTER_KEY" AS committee_id,
        'OH_'
        || {{ dbt_utils.generate_surrogate_key([
              'candidate_name',
              'candidate_party'
            ]) }} AS candidate_id,
        NULL AS committee_designation,
        "COM_NAME" AS "name",
        "COM_ADDRESS" AS address1,
        NULL AS address2,
        "COM_CITY" AS city,
        "COM_STATE" AS "state",
        "COM_ZIP" AS zip_code,
        "PARTY" AS affiliation,
        "DISTRICT" AS district,
        current_timestamp AS insert_datetime
    FROM candidate_active_source

    UNION ALL

    SELECT
        'OH' AS "source",
        'OH_CAN_' || "MASTER_KEY" AS committee_id,
        'OH_'
        || {{ dbt_utils.generate_surrogate_key([
              'candidate_name',
              'candidate_party'
            ]) }} AS candidate_id,
        NULL AS committee_designation,
        "COM_NAME" AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        "PARTY" AS affiliation,
        "DISTRICT" AS district,
        current_timestamp AS insert_datetime
    FROM candidate_inactive_source
),

committee_transformed AS (
    SELECT
        'OH' AS "source",
        'OH_COM_' || "MASTER_KEY" AS committee_id,
        NULL AS candidate_id,
        NULL AS committee_designation,
        "COM_NAME" AS "name",
        "COM_ADDRESS" AS address1,
        NULL AS address2,
        "COM_CITY" AS city,
        "COM_STATE" AS state,
        "COM_ZIP" AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        current_timestamp AS insert_datetime
    FROM {{ source('oh', 'oh_committee_list_landing') }}

    UNION ALL

    SELECT DISTINCT
        'OH' AS "source",
        'OH_COM_' || "MASTER_KEY" AS committee_id,
        NULL AS candidate_id,
        NULL AS committee_designation,
        "COM_NAME" AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        current_timestamp AS insert_datetime
    FROM {{ source('oh', 'oh_committee_contribution_landing') }}
),

transformed AS (
    SELECT * FROM candidate_transformed

    UNION ALL

    SELECT * FROM committee_transformed
),

numbered AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY committee_id) AS row_num
    FROM transformed
)

SELECT * FROM numbered
WHERE row_num = 1

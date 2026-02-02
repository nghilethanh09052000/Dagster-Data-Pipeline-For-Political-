{{ config(
    materialized='table',
    tags=["north_carolina", "committee", "master"]
) }}

WITH source AS (
    SELECT
        "CommitteeSBoEID" AS "id",
        "CommitteeName" AS "name",
        "CommitteeStreet1" AS address1,
        "CommitteeStreet2" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code
    FROM {{ source('nc', 'nc_transactions_landing') }}
    WHERE
        "CommitteeName" IS NOT NULL
        AND "CommitteeName" <> ''
        AND "CommitteeSBoEID" IS NOT NULL
        AND "CommitteeSBoEID" <> ''
),

deduplicated AS (
    SELECT
        "id",
        MAX(name) AS "name",
        MAX(address1) AS address1,
        MAX(address2) AS address2,
        MAX(city) AS city,
        MAX(state) AS "state",
        MAX(zip_code) AS zip_code
    FROM source
    GROUP BY "id"
),

transformed AS (
    SELECT
        'NC' AS "source",
        'NC_' || "id" AS committee_id,
        NULL AS committee_designation,
        name,
        address1,
        address2,
        city,
        state,
        zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM deduplicated
)

SELECT * FROM transformed

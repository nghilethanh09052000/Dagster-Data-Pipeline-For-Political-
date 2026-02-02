{{ config(
    materialized='table',
    tags=["pennsylvania", "committee", "master"]
) }}

WITH ranked_source_data AS (
    SELECT
        filerid,
        filertype,
        filername,
        address1,
        address2,
        city,
        state,
        zipcode,
        party,
        district,
        submitteddate,
        ROW_NUMBER()
            OVER (
                PARTITION BY filerid
                ORDER BY submitteddate DESC
            )
            AS row_num
    FROM {{ source('pa', 'pa_filer_new_format_landing_table') }}
    WHERE
        filerid IS NOT NULL
        AND filername IS NOT NULL
),

ranked_source_data_old AS (
    SELECT
        "FilerIdentificationNumber",
        "FilerName",
        "FilerAddress1",
        "FilerAddress2",
        "FilerCity",
        "FilerState",
        "FilerZipCode",
        "FilerParty",
        ROW_NUMBER() OVER (PARTITION BY "FilerIdentificationNumber") AS row_num
    FROM {{ source('pa', 'pennsylvania_old_filer_landing') }}
    WHERE "Amendment" = 'N'
)

SELECT
    'PA' AS "source",
    'PA_' || filerid AS committee_id,
    NULL AS committee_designation,
    filername AS "name",
    address1,
    address2,
    city,
    state,
    zipcode AS zip_code,
    party AS affiliation,
    district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM ranked_source_data
WHERE row_num = 1

UNION ALL

SELECT
    'PA' AS "source",
    'PA_OLD_' || "FilerIdentificationNumber" AS committee_id,
    NULL AS committee_designation,
    "FilerName" AS "name",
    "FilerAddress1" AS address1,
    "FilerAddress2" AS address2,
    "FilerCity" AS city,
    "FilerState" AS "state",
    "FilerZipCode" AS zip_code,
    "FilerParty" AS affiliation,
    NULL AS district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM ranked_source_data_old
WHERE row_num = 1

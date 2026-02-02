{{
    config(
      materialized='table',
      tags=["pennsylvania", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        campaignfinanceid,
        filerid,
        contributor,
        city,
        state,
        zipcode,
        ename,
        occupation,
        contamt1,
        contdate1
    FROM {{ source('pa', 'pa_contrib_new_format_landing_table') }}
),

source_data_old AS (
    SELECT DISTINCT
        "FilerIdentificationNumber",
        "Year",
        "ReportCycleCode",
        "SectionCode",
        "ContributorName",
        "ContributorAddress1",
        "ContributorAddress2",
        "ContributorCity",
        "ContributorState",
        "ContributorZipCode",
        "ContributorOccupation",
        "EmployerName",
        "EmployerAddress1",
        "EmployerAddress2",
        "EmployerCity",
        "EmployerState",
        "EmployerZipCode",
        "ContributionDate1",
        "ContributionAmount1"
    FROM {{ source('pa', 'pennsylvania_old_contrib_landing') }}
)

SELECT
    'PA' AS "source",
    'PA_' || {{
        dbt_utils.generate_surrogate_key(
            [
                "campaignfinanceid",
                "filerid",
                "contributor",
                "city",
                "state",
                "zipcode",
                "ename",
                "occupation",
                "contamt1",
                "contdate1"
        ]) }} AS source_id,
    'PA_' || filerid AS committee_id,
    contributor AS "name",
    city,
    state,
    zipcode AS zip_code,
    ename AS employer,
    occupation,
    CAST(contamt1 AS numeric) AS amount,
    CASE
        WHEN contdate1 IS NOT NULL AND TRIM(contdate1) != ''
            THEN TO_TIMESTAMP(contdate1, 'YYYYMMDD')
    END AS contribution_datetime,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE
    contamt1 IS NOT NULL
    AND contributor IS NOT NULL

UNION ALL

SELECT
    'PA' AS "source",
    'PA_OLD_' || {{
        dbt_utils.generate_surrogate_key(
            [
                '"FilerIdentificationNumber"',
                '"Year"',
                '"ReportCycleCode"',
                '"SectionCode"',
                '"ContributorName"',
                '"ContributorAddress1"',
                '"ContributorAddress2"',
                '"ContributorCity"',
                '"ContributorState"',
                '"ContributorZipCode"',
                '"ContributorOccupation"',
                '"EmployerName"',
                '"EmployerAddress1"',
                '"EmployerAddress2"',
                '"EmployerCity"',
                '"EmployerState"',
                '"EmployerZipCode"',
                '"ContributionDate1"',
                '"ContributionAmount1"'
        ]) }} AS source_id,
    'PA_OLD_' || "FilerIdentificationNumber" AS committee_id,
    "ContributorName" AS "name",
    "ContributorCity" AS city,
    "ContributorState" AS "state",
    "ContributorZipCode" AS zip_code,
    "EmployerName" AS employer,
    "ContributorOccupation" AS occupation,
    CAST("ContributionAmount1" AS numeric) AS amount,
    TO_TIMESTAMP("ContributionDate1", 'YYYYMMDD') AS contribution_datetime,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data_old
WHERE
    "ContributionDate1" ~ '^[0-9]{8}$'

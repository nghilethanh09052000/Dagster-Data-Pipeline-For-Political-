{{
    config(
      materialized='table',
      tags=["maryland", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "ReceivingCommittee",
        "FilingPeriod",
        "ContributionDate",
        "ContributorName",
        "ContributorAddress",
        "ContributionType",
        "ContributionAmount",
        "EmployerName",
        "EmployerOccupation",
        "Office",
        "Fundtype"
    FROM
        {{ source('md', 'md_contributions_and_loans_landing') }}
    WHERE
        "ContributorType" = 'Individual'
    GROUP BY
        "ReceivingCommittee",
        "FilingPeriod",
        "ContributionDate",
        "ContributorName",
        "ContributorAddress",
        "ContributionType",
        "ContributionAmount",
        "EmployerName",
        "EmployerOccupation",
        "Office",
        "Fundtype"
),

address_transformed AS (
    SELECT
        *,
        -- Normalize the whitespace to single space
        REGEXP_REPLACE("ContributorAddress", '\s+', ' ', 'g') AS normalized_address,

        -- Extract ZIP: last token that looks like a zip
        REGEXP_MATCHES("ContributorAddress", '(\d{5}(?:-\d{4})?)') AS zip_match,

        -- Extract state: 2-letter code before zip
        REGEXP_MATCHES("ContributorAddress", '\s([A-Z]{2})\s+\d{5}(?:-\d{4})?') AS state_match
    FROM source_data
),

transformed_data AS (
    SELECT
        'MD' AS "source",
        'MD_' || {{ dbt_utils.generate_surrogate_key([
                '"ReceivingCommittee"',
                '"FilingPeriod"',
                '"ContributionDate"',
                '"ContributorName"',
                '"ContributorAddress"',
                '"ContributionType"',
                '"ContributionAmount"',
                '"EmployerName"',
                '"EmployerOccupation"',
                '"Office"',
                '"Fundtype"'
        ]) }} AS source_id,
        'MD_' || {{ dbt_utils.generate_surrogate_key(['"ReceivingCommittee"']) }} AS committee_id,
        "ContributorName" AS "name",

        -- No easy guess or consistent enough pattern to get the city
        NULL AS city,

        -- Safely extract state from regex match array
        CASE
            WHEN state_match IS NOT NULL AND array_length(state_match, 1) > 0 THEN state_match[1]
            ELSE NULL
        END AS state,

        -- Safely extract zip from regex match array
        CASE
            WHEN zip_match IS NOT NULL AND array_length(zip_match, 1) > 0 THEN zip_match[1]
            ELSE NULL
        END AS zip_code,

        "EmployerName" AS employer,
        "EmployerOccupation" AS occupation,
        "ContributionAmount"::decimal AS amount,
        TO_TIMESTAMP("ContributionDate", 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM address_transformed
)


SELECT
    source,
    source_id,
    committee_id,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM transformed_data

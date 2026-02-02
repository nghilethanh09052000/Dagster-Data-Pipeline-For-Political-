{{
    config(
        materialized='table',
        tags=["mississippi", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "ReferenceNumber",
        "FilingId",
        "Contributor",
        "Recipient",
        -- Sum the amount to aggregate
        SUM(
          {{ clean_dolar_sign_amount('"Amount"') }}
        ) AS "Amount",
        -- Below take just one of the data, as the data is now deduped
        -- for ReferenceNumber, FilingId, Contributor, and Recipient
        MIN("AddressLine1") AS "AddressLine1",
        MIN("City") AS "City",
        MIN("StateCode") AS "StateCode",
        MIN("PostalCode") AS "PostalCode",
        MIN("Occupation") AS "Occupation",
        MIN("Date") AS "Date"
    FROM {{ source('ms', 'ms_contributions_landing') }}
    -- Remove contributions from PACs and others
    WHERE "ContributorType" = 'Individual'
    GROUP BY
        "ReferenceNumber",
        "FilingId",
        "Contributor",
        "Recipient"
),

cleaned_data AS (
    SELECT
        'MS' AS "source",
        'MS_' || "ReferenceNumber" || '_' || "FilingId" || '_' || "Contributor" || '_' || "Recipient" AS source_id,
        (
            SELECT committee_id
            FROM {{ ref('mississippi_committee_master_transform')}}
            WHERE "name" = "source_data"."Recipient"
            LIMIT 1
        ) AS committee_id,
        "Contributor" AS "name",
        "City" AS city,
        "StateCode" AS "state",
        "PostalCode" AS zip_code,
        NULL AS employer,
        "Occupation" AS occupation,
        "Amount" AS amount,
        to_timestamp("Date", 'MM/DD/YYYY HH:MI:SS AM') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
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
FROM cleaned_data

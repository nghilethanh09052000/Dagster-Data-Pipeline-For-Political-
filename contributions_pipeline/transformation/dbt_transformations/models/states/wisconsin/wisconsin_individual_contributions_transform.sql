{{ 
    config(
        materialized='table',
        tags=["wisconsin", "contributions", "master"]
    ) 
}}

WITH source AS (
    SELECT DISTINCT
        transaction_date,
        contributor_name,
        contribution_amount,
        address_line1,
        address_line2,
        city,
        state_code,
        zip,
        occupation,
        employer_name,
        employer_address,
        contributor_type,
        receiving_committee_name,
        ethcfid
    FROM {{ source('wi', 'wi_receipt_transactions_landing') }}
),

transformed AS (
    SELECT
        'WI' AS source,
        'WI_' || {{ dbt_utils.generate_surrogate_key(
            [
                "transaction_date",
                "contributor_name",
                "contribution_amount",
                "address_line1",
                "address_line2",
                "city",
                "state_code",
                "zip",
                "occupation",
                "employer_name",
                "employer_address",
                "contributor_type",
                "receiving_committee_name",
                "ethcfid"
            ]
        ) }} AS source_id,
        'WI_' || ethcfid AS committee_id, 
        contributor_name AS name,
        city,
        state_code AS state,
        zip as zip_code,
        employer_name AS employer,
        occupation,
        CAST(contribution_amount AS NUMERIC) AS amount, 
        transaction_date::timestamp AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source
)

SELECT * FROM transformed

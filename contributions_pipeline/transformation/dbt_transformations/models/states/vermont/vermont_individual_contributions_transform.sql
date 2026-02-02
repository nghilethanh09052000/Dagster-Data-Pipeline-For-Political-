{{ 
    config(
        materialized='table',
        tags=["vermont", "contributions", "master"]
    ) 
}}

WITH source_data AS (
    SELECT DISTINCT
        a."TransactionId" AS transaction_id,
        a."FilingEntityId" AS filing_entity_id,
        b.filing_entity_id AS candidate_id,
        CONCAT(a."ContributorFirstName", ' ', a."ContributorLastName") AS contributor_name,
        CONCAT_WS(' ', b.candidate_first_name, b.candidate_middle_name, b.candidate_last_name) as candidate_name,
        a."ContributorAddressCity" AS contributor_city,
        a."ContributorAddressState" AS contributor_state,
        a."ContributorAddressZipCode" AS contributor_zip_code,
        a."TransactionAmount" AS transaction_amount,
        a."TransactionDate" AS transaction_date
    FROM {{ source('vt', 'vermont_contributions_landing') }} a
    LEFT JOIN {{ source('vt', 'vermont_candidates_landing') }} b
    ON a."FilingEntityId" = b.filing_entity_id
    WHERE "TransactionId" IS NOT NULL
      AND "TransactionAmount" IS NOT NULL
      AND "TransactionDate" IS NOT NULL
),

transformed_data AS (
    SELECT DISTINCT
        'VT' AS source,
        'VT_' || {{ dbt_utils.generate_surrogate_key([
            "transaction_id", 
            "filing_entity_id",
            "contributor_city",
            "contributor_state",
            "contributor_zip_code",
            "contributor_name", 
            "transaction_amount", 
            "transaction_date"
        ]) }} AS source_id,
        'VT_' || filing_entity_id AS committee_id,
        contributor_name AS name,
        'VT_'  || candidate_id as candidate_id,
        candidate_name,
        contributor_city AS city,
        contributor_state AS state,
        contributor_zip_code AS zip_code,
        NULL AS employer,  
        NULL AS occupation,  
        {{
            clean_dolar_sign_amount('transaction_amount')
        }} as amount,
        CAST(transaction_date AS TIMESTAMP) AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT * FROM transformed_data
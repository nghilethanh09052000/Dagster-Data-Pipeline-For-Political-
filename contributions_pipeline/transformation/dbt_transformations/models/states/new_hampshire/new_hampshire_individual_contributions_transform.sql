{{
    config(
      materialized='table',
      tags=["new_hampshire", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'NH' AS source,
        'NH_' || {{ dbt_utils.generate_surrogate_key([
            '"FilingEntityID"', 
            '"ContributorCity"', 
            '"ContributorState"', 
            '"ContributorZipCode"', 
            '"Contributoroccupation"', 
            '"ContributorEmployer"', 
            '"Amountofreceipt"', 
            '"DateofReceipt"'
        ]) }} AS source_id,
        'NH_' || "FilingEntityID" AS committee_id,
        "ContributorName" AS name,
        "ContributorCity" AS city,
        "ContributorState" AS state,
        "ContributorZipCode" AS zip_code,
        "ContributorEmployer" AS employer,
        "Contributoroccupation" AS occupation,
        {{ clean_dolar_sign_amount('"Amountofreceipt"') }} AS amount,
        TO_TIMESTAMP("DateofReceipt", 'MM/DD/YYYY') AS contribution_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY 
                "FilingEntityID", "ContributorCity", "ContributorState", 
                "ContributorZipCode", "Contributoroccupation", 
                "ContributorEmployer", "Amountofreceipt", "DateofReceipt"
        ) AS row_num
    FROM {{ source('nh', 'nh_new_receipts_landing') }}
    WHERE
        "TransactionType" = 'Receipt'
        AND "ContributorType" = 'Individual / Candidate'
),

committee_to_candidate AS (
    SELECT DISTINCT
        'NH_' || cc."filerEntityId" AS candidate_id,
        CONCAT_WS(' ', cc."candidateFirstName", cc."candidateLastName") AS candidate_name,
        'NH_' || pc."filingEntityId" AS committee_id
    FROM {{ source('nh', 'nh_candidate_committee_landing') }} cc
    JOIN {{ source('nh', 'nh_political_committee_landing') }} pc
        ON pc."filingEntityId" = cc."filerEntityId"
    WHERE pc."filingEntityId" <> 'Filing Entity Id'
),

final AS (
    SELECT
        s.source,
        s.source_id,
        s.committee_id,
        c.candidate_id,
        c.candidate_name,
        s.name,
        s.city,
        s.state,
        LEFT(s.zip_code, 5) AS zip_code,
        s.employer,
        s.occupation,
        s.amount,
        s.contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data s
    LEFT JOIN committee_to_candidate c
        ON s.committee_id = c.committee_id
    WHERE s.amount >= 0
      AND s.row_num = 1
)

SELECT * FROM final

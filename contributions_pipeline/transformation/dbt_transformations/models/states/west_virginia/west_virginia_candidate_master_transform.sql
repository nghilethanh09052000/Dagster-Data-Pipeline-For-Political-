{{ 
  config(
    materialized='table',
    tags=["west_virginia", "candidates", "master"]
  ) 
}}

WITH source_data AS (
    SELECT
        "IDNumber",
        "CandidateName",
        "ElectionYear",
        "Party",
        "CandidateAddress",
        "OfficeName",
        "District",
        "Status",
        "FinanceType",
        "RegistrationDate",
        "TreasurerName",
        "PublicPhoneNumber",
        "PoliticalPartyCommitteeName",
        "CandidateStatus",
        "Incumbent",
        "UnregisteredCandidate",
        "CashOnHand",
        "TotalContributions",
        "TotalExpenditures",
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('wv', 'wv_candidates_landing') }}
    WHERE "CandidateName" IS NOT NULL AND TRIM("CandidateName") <> ''
),
transformed_data AS (
    SELECT
        'WV' AS source,
        'WV_' || "IDNumber" AS candidate_id,
        "CandidateName" AS name,
        "Party" AS affiliation,
        "ElectionYear" AS election_year,
        "OfficeName" AS office_sought,
        "District" AS district,
        "Status" AS status,
        "FinanceType" AS finance_type,
        "RegistrationDate" AS registration_date,
        "TreasurerName" AS treasurer_name,
        "CandidateAddress" AS address,
        "PublicPhoneNumber" AS phone_number,
        "PoliticalPartyCommitteeName" AS committee_name,
        "CandidateStatus" AS candidate_status,
        "Incumbent" AS is_incumbent,
        "UnregisteredCandidate" AS is_unregistered,
        "CashOnHand" AS cash_on_hand,
        "TotalContributions" AS total_contributions,
        "TotalExpenditures" AS total_expenditures,
        insert_datetime
    FROM source_data
),
ranked_data AS (
    SELECT
        source,
        candidate_id,
        name,
        affiliation,
        election_year,
        office_sought,
        district,
        status,
        finance_type,
        registration_date,
        treasurer_name,
        address,
        phone_number,
        committee_name,
        candidate_status,
        is_incumbent,
        is_unregistered,
        cash_on_hand,
        total_contributions,
        total_expenditures,
        insert_datetime,
        ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY election_year DESC) AS row_num
    FROM transformed_data
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    election_year,
    office_sought,
    district,
    status,
    finance_type,
    registration_date,
    treasurer_name,
    address,
    phone_number,
    committee_name,
    candidate_status,
    is_incumbent,
    is_unregistered,
    cash_on_hand,
    total_contributions,
    total_expenditures,
    insert_datetime
FROM ranked_data
WHERE row_num = 1
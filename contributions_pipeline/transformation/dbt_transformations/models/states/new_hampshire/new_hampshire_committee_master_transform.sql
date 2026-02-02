{{ 
    config(
      materialized='table',
      tags=["new_hampshire", "committee", "master"]
    ) 
}}

WITH political_committee AS (
    SELECT
        'NH' AS source,
        'NH_' || pc."filingEntityId" AS committee_id,
        'NH_' || cc."filerEntityId" AS candidate_id,  
        CASE pc."committeeSubtype"
            WHEN 'Candidate Committee' THEN 'P'
            WHEN 'Major Purpose Electioneering' THEN 'U'
            WHEN 'Major Purpose NOT Electioneering' THEN 'U'
            WHEN 'Political Committee of a Political Party' THEN 'U'
            WHEN 'Segregated Organization Fund' THEN 'U'
        END AS committee_designation,
        COALESCE(cc."candidateCommitteeName", pc."committeeName") AS name,
        pc."committeeAddressLine1" AS address1,
        pc."committeeAddressLine2" AS address2,
        pc."committeeCity" AS city,
        pc."committeeState" AS state,
        pc."committeeZipCode" AS zip_code,
        cc."politicalParty" AS affiliation,
        cc."officeDistrictName" AS district,
        CONCAT_WS(' ', cc."candidateFirstName", cc."candidateLastName") AS candidate_name
    FROM {{ source('nh', 'nh_political_committee_landing') }} pc
    LEFT JOIN {{ source('nh', 'nh_candidate_committee_landing') }} cc
        ON pc."filingEntityId" = cc."filerEntityId"
    WHERE pc."filingEntityId" <> 'Filing Entity Id'
),

candidate_committee AS (
    SELECT
        'NH' AS source,
        'NH_' || "filerEntityId" AS committee_id,
        'NH_' || "filerEntityId" AS candidate_id,
        'P' AS committee_designation,   -- always candidate committee
        "candidateCommitteeName" AS name,
        "committeeAddressLine1" AS address1,
        "committeeAddressLine2" AS address2,
        "committeeCity" AS city,
        "committeeState" AS state,
        "committeeZipCode" AS zip_code,
        "politicalParty" AS affiliation,
        "officeDistrictName" AS district,
        CONCAT_WS(' ', "candidateFirstName", "candidateLastName") AS candidate_name
    FROM {{ source('nh', 'nh_candidate_committee_landing') }}
    WHERE "filerEntityId" <> 'Filer Entity Id'
),

all_committees AS (
    SELECT * FROM political_committee
    UNION ALL
    SELECT * FROM candidate_committee
),

deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY committee_id
        ) AS rn
    FROM all_committees
)

SELECT
    source,
    committee_id,
    candidate_id,
    candidate_name,
    committee_designation,
    name,
    address1,
    address2,
    city,
    state,
    LEFT(zip_code, 5) AS zip_code,
    affiliation,
    district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM deduped
WHERE rn = 1

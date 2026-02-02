{{
    config(
      materialized='table',
      tags=["new_hampshire", "candidate", "master"]
    )
}}

WITH candidate_landing AS (
    SELECT
        'NH' AS source,
        'NH_' || "filerEntityId" as candidate_id,
        concat_ws(' ', "candidateFirstName", "candidateLastName") as name,
        "politicalParty" AS affiliation,
        "officeDistrictName" AS district, 
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "filerEntityId"
        ) as rn
    FROM {{ source('nh', 'nh_candidate_landing') }}
    WHERE "filerEntityId" <> 'Filer Entity Id'
),

candidate_committee_landing AS (
    SELECT
        'NH' AS source,
        'NH_' || "filerEntityId" as candidate_id,
        concat_ws(' ', "candidateFirstName", "candidateLastName") as name,
        "politicalParty" AS affiliation,
        "officeDistrictName" AS district, 
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "filerEntityId"
        ) as rn
    FROM {{ source('nh', 'nh_candidate_committee_landing') }}
    WHERE "filerEntityId" <> 'Filer Entity Id'
)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM candidate_landing
WHERE rn = 1

UNION

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM candidate_committee_landing
WHERE rn = 1

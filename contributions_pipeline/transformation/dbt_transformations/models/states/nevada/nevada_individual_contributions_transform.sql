{{
    config(
      materialized='table',
      tags=["nevada", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'NV' AS "source",
        'NV_' || cont."ContributionID" AS source_id,
        'NV_'
        || CASE
            WHEN
                cont."CandidateID" IS NOT NULL
                AND TRIM(cont."CandidateID") != ''
                THEN 'CAND' || cont."CandidateID"
            ELSE 'GRP_' || cont."GroupID"
        END AS committee_id,
        cm.candidate_id,
        cm."name" AS candidate_name,
        COALESCE(NULLIF(pay."FirstName", '') || ' ', '')
        || COALESCE(NULLIF(pay."MiddleName", '') || ' ', '')
        || pay."LastName" AS "name",
        pay."City" AS city,
        pay."State" AS "state",
        pay."Zip" AS zip_code,
        NULL AS employer,
        NULL AS occupation,
        cont."ContributionAmount"::decimal AS amount,
        TO_TIMESTAMP(cont."ContributionDate", 'MM/DD/YYYY')
            AS contribution_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "ContributionID"
        ) AS row_num
    FROM {{ source('nv', 'nv_contributions_landing') }} AS cont
    INNER JOIN {{ source('nv', 'nv_contributors_payees_landing') }} AS pay
        ON cont."ContributorID" = pay."ContactID"
    LEFT JOIN {{ ref('nevada_candidate_master_transform') }} AS cm
        ON cont."CandidateID" = cm."CandidateID"
    WHERE
        (cont."CandidateID" IS NOT NULL AND TRIM(cont."CandidateID") != '')
        OR (cont."GroupID" IS NOT NULL AND TRIM(cont."GroupID") != '')
)

SELECT
    source,
    source_id,
    committee_id,
    candidate_id,
    candidate_name,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE
    amount >= 0
    AND row_num = 1

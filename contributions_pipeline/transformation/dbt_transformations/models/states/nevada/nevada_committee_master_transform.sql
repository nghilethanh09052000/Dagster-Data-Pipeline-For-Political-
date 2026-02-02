{{
    config(
      materialized='table',
      tags=["nevada", "committee", "master"]
    )
}}

WITH source_data AS (
    -- We still need to add the candidate here as the "CandidateID" and
    -- "GroupID" are mutually exclusive on contributions table, if one
    -- is null/empty then the other one is not. Thus I assume this
    -- mean the candidate here acts kind of like a committee as well.
    SELECT
        'NV' AS "source",
        'NV_CAND_' || "CandidateID" AS committee_id,
        'NV_' || "CandidateID" AS candidate_id,
        -- Or maybe this should be authorized PAC?
        NULL AS committee_designation,
        COALESCE(NULLIF("FirstName", '') || ' ', '') || "LastName" AS "name",
        "MailingAddress" AS address1,
        NULL AS address2,
        "MailingCity" AS city,
        "MailingState" AS "state",
        "MailingZip" AS zip_code,
        "Party" AS affiliation,
        NULL AS district,
        ROW_NUMBER() OVER (
            PARTITION BY "CandidateID"
        ) AS row_num
    FROM {{ source('nv', 'nv_candidates_landing') }}

    UNION ALL

    SELECT
        'NV' AS "source",
        'NV_GRP_' || "GroupID" AS committee_id,
        NULL AS candidate_id,
        CASE "GroupType"
            WHEN 'Political Action Committee' THEN 'U'
            WHEN 'Political Party Committee' THEN 'U'
            WHEN 'Recall Committee' THEN 'U'
            WHEN 'Non-Profit Corporation' THEN 'U'
            WHEN 'Independent Expenditure' THEN 'U'
            WHEN 'PAC Ballot Advocacy Group' THEN 'U'
        END AS committee_designation,
        "GroupName" AS "name",
        NULL AS address1,
        NULL AS address2,
        "City" AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        ROW_NUMBER() OVER (
            PARTITION BY "GroupID"
        ) AS row_num
    FROM {{ source('nv', 'nv_groups_landing') }}
)

SELECT
    source,
    committee_id,
    candidate_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM source_data
WHERE row_num = 1

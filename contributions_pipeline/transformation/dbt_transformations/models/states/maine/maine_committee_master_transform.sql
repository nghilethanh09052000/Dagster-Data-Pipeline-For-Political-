{{
    config(
        materialized='table',
        tags=["maine", "committee", "master"]
    )
}}

WITH source_data_old AS (
    -- Same as yours
    SELECT
        "OrgID",
        "CommitteeName",
        "CandidateName",
        CASE
            WHEN
                "CommitteeName" IS NULL OR TRIM("CommitteeName") = ''
                THEN "CandidateName"
            ELSE "CommitteeName"
        END AS full_name,
        "CommitteeType",
        ROW_NUMBER() OVER (
            PARTITION BY "OrgID"
        ) AS row_num
    FROM {{ source('me', 'maine_old_contributions_and_loans_landing') }}
    WHERE
        ("CommitteeName" IS NOT NULL AND TRIM("CommitteeName") != '')
        OR ("CandidateName" IS NOT NULL AND TRIM("CandidateName") != '')
    GROUP BY
        "OrgID",
        "CommitteeName",
        "CandidateName",
        "CommitteeType"
),

source_data_new AS (
    -- Same as yours
    SELECT
        "OrgID",
        "LegacyID",
        "CommitteeName",
        "CandidateName",
        CASE
            WHEN
                "CommitteeName" IS NULL OR TRIM("CommitteeName") = ''
                THEN "CandidateName"
            ELSE "CommitteeName"
        END AS full_name,
        "CommitteeType"
    FROM {{ source('me', 'maine_new_contributions_and_loans_landing') }}
    WHERE
        ("CommitteeName" IS NOT NULL AND TRIM("CommitteeName") != '')
        OR ("CandidateName" IS NOT NULL AND TRIM("CandidateName") != '')
    GROUP BY
        "OrgID",
        "LegacyID",
        "CommitteeName",
        "CandidateName",
        "CommitteeType"
),

deduped_old AS (
    -- Keep old records ONLY if their OrgID is NOT a LegacyID in new
    SELECT old.*
    FROM source_data_old old
    LEFT JOIN source_data_new new
        ON old."OrgID" = new."LegacyID"
    WHERE new."LegacyID" IS NULL AND old.row_num = 1
),

transformed_data AS (
    SELECT
        'ME' AS "source",
        'ME_OLD_' || "CommitteeType" || '_' || "OrgID" AS committee_id,
        CASE "CommitteeType"
            WHEN 'Candidate' THEN 'P'
            WHEN 'Party Committee' THEN 'U'
            WHEN 'Political Action Committee' THEN 'U'
            WHEN 'Ballot Question Committee' THEN 'U'
        END AS committee_designation,
        full_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM deduped_old

    UNION ALL

    SELECT
        'ME' AS "source",
        'ME_NEW_' || "CommitteeType" || '_' || "OrgID" AS committee_id,
        CASE "CommitteeType"
            WHEN 'Candidate' THEN 'P'
            WHEN 'Party Committee' THEN 'U'
            WHEN 'Political Action Committee' THEN 'U'
            WHEN 'Ballot Question Committee' THEN 'U'
        END AS committee_designation,
        full_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data_new
)

SELECT
    "source",
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM transformed_data

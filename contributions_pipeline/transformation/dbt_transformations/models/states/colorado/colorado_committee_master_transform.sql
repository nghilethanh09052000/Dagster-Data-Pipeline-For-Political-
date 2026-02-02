{{
    config(
        materialized='table',
        tags=["colorado", "committee", "master"]
    )
}}

-- Landing table docs: http://tracer.sos.colorado.gov/PublicSite/Resources/DownloadDataFileKey.pdf

WITH source_data AS (
    SELECT
        "CO_ID",
        "CommitteeType",
        "CommitteeName",
        "CandidateName",
        ROW_NUMBER() OVER (
            PARTITION BY "CO_ID"
        ) AS rn
    FROM {{ source('co', 'co_contributions_landing') }}
    WHERE 
        "CO_ID" <> '' 
),

deduplicated AS (
    SELECT
        'CO' as source,
        'CO_' || "CO_ID" AS committee_id,
        CASE 
            WHEN "CandidateName" <> '' 
                THEN 'CO_' || "CO_ID" 
            ELSE NULL
        END AS candidate_id,
        NULLIF("CandidateName", '') AS candidate_name,
        CASE "CommitteeType"
            WHEN 'Candidate' THEN 'P'
            WHEN 'Candidate Committee' THEN 'P'
            WHEN 'Political Party Committee' THEN 'U'
            WHEN 'Political Committee' THEN 'U'
            WHEN 'Federal PAC' THEN 'U'
            WHEN 'Small Donor Committee' THEN 'U'
            WHEN 'Small Scale Issue Committee' THEN 'U'
            WHEN 'Issue Committee' THEN 'U'
            WHEN '527 Political Organization' THEN 'U'
        END AS committee_designation,
        "CommitteeName" AS name,
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS state,
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE rn = 1
)

SELECT * FROM deduplicated

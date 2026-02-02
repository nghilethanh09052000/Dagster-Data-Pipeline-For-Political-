{{
    config(
      materialized='table',
      tags=["california", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        filer_id,
        entity_cd,
        filer_naml,
        filer_namf,
        filer_city,
        filer_st,
        filer_zip4,
        juris_cd,
        juris_dscr,
        dist_no,
        cand_naml,
        cand_namf,
        ROW_NUMBER() OVER (
            PARTITION BY filer_id
        ) AS row_num
    FROM {{ source('ca', 'ca_cvr_campaign_disclosure') }}
    WHERE report_num::int = 0
),

cleaned_data AS (
    SELECT
        'CA' AS "source",
        filer_id,
        'CA_' || filer_id AS committee_id,
        -- Need to crate surrogate key as there is cand_id
        -- on ca_cvr_campaign_disclosure, but literally only
        -- one record have it
        'CA_' || {{ dbt_utils.generate_surrogate_key(['cand_namf', 'cand_naml']) }} as candidate_id,
        coalesce(nullif(cand_namf, '') || ' ', '') || cand_naml as candidate_name,
        CASE entity_cd
            -- Candidate/Office-holder as Authorized by candidate
            WHEN 'CAO' THEN 'A'
            -- Controlled Committee as Principal campaign committee
            WHEN 'CTL' THEN 'P'
            WHEN 'RCP' THEN 'U' -- Recipient Committee as Unauthorized
            WHEN 'BMC' THEN 'U' -- Ballot Measure Committee as Unauthorized
            WHEN 'MDI' THEN 'U' -- Major Donor as Unauthorized
            WHEN 'SMO' THEN 'U' -- Slate Mailer Organization as Unauthorized)
        END AS committee_designation,
        CASE
            WHEN filer_namf IS NULL OR TRIM(filer_namf) = '' THEN filer_naml
            ELSE filer_namf || ' ' || filer_naml
        END AS "name",
        NULL AS address1, -- No address available from the exported data
        NULL AS address2,
        filer_city AS city,
        filer_st AS "state",
        filer_zip4 AS zip_code,
        NULL AS affiliation,
        CASE
            WHEN
                juris_cd IN ('SEN', 'ASM', 'BOE') AND dist_no IS NOT NULL
                THEN juris_dscr || ' - ' || dist_no
            WHEN juris_dscr IS NOT NULL THEN juris_dscr
        END AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE row_num = 1
)

SELECT
    source,
    filer_id,
    committee_id,
    candidate_id,
    candidate_name,
    committee_designation,
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM cleaned_data

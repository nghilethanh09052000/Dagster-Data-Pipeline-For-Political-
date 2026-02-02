{{
    config(
      materialized='table',
      tags=["texas", "contributions", "master"]
    )
}}

-- Tables docs: https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt
-- Codes (enums) docs: https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt

WITH source_data AS (
    SELECT
        cm.candidate_id,
        cm.candidate_name,
        TRIM(
            COALESCE(NULLIF(cl.contributor_name_prefix_cd, '') || ' ', '')
            || cl.contributor_name_first || ' '
            || cl.contributor_name_last
            || CASE
                WHEN
                    cl.contributor_name_suffix_cd IS NOT NULL
                    AND cl.contributor_name_suffix_cd <> ''
                    THEN ' ' || cl.contributor_name_suffix_cd
                ELSE ''
            END
        ) AS full_name,
        cl.record_type,
        cl.filer_ident,
        cl.contribution_info_id,
        cl.contribution_dt,
        cl.contribution_amount,
        cl.contributor_street_city,
        cl.contributor_street_state_cd,
        cl.contributor_street_postal_code,
        cl.contributor_employer,
        cl.contributor_occupation
    FROM {{ source('tx', 'tx_contribs_landing') }} AS cl
    LEFT JOIN {{ ref('texas_committee_master_transform') }} AS cm
        ON cl.filer_ident = cm.filer_ident
    WHERE
        cl.contributor_persent_type_cd = 'INDIVIDUAL'
),

transformed_data AS (
    SELECT
        'TX' AS "source",
        -- Pledge (PLDG) and Receipt (RCPT)
        'TX_' || record_type || '_' || contribution_info_id AS source_id,
        'TX_' || filer_ident AS committee_id,
        candidate_id,
        candidate_name,
        full_name AS "name",
        contributor_street_city AS city,
        contributor_street_state_cd AS "state",
        contributor_street_postal_code AS zip_code,
        contributor_employer AS employer,
        contributor_occupation AS occupation,
        contribution_amount::decimal AS amount,
        TO_TIMESTAMP(contribution_dt, 'YYYYMMDD') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
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
    insert_datetime
FROM transformed_data

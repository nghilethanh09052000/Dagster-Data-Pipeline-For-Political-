{{
    config(
      materialized='table',
      tags=["texas", "committee", "master"]
    )
}}

-- Tables docs: https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt
-- Codes (enums) docs: https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt

WITH source_data AS (
    SELECT
        ct.candidate_id,
        ct."name" AS candidate_name,
        fl.filer_ident,
        fl.filer_name,
        fl.filer_type_cd,
        fl.filer_street_addr1,
        fl.filer_street_addr2,
        fl.filer_street_city,
        fl.filer_street_state_cd,
        fl.filer_street_postal_code,
        fl.cta_seek_office_district,
        -- Each filer ident could be multiple type, but we'll just take one
        ROW_NUMBER() OVER (
            PARTITION BY fl.filer_ident
        ) AS row_num
    FROM {{ source('tx', 'tx_filers_landing') }} AS fl
    LEFT JOIN {{ source('tx', 'tx_spacs_landing') }} AS spl
        ON
            fl.filer_ident = spl.spac_filer_ident
            AND spl.spac_position_cd = 'SUPPORT'
    LEFT JOIN {{ ref('texas_candidate_master_transform') }} AS ct
        ON spl.candidate_filer_ident = ct.filer_ident
    WHERE fl.filer_type_cd != 'COH' OR fl.filer_type_cd != 'JCOH'
),

transformed_data AS (
    SELECT
        'TX' AS "source",
        filer_ident,
        'TX_' || filer_ident AS committee_id,
        candidate_id,
        candidate_name,
        CASE
            -- Principal campaign committees
            WHEN filer_type_cd IN ('SCC', 'SPK') THEN 'P'

            -- Authorized committees supporting specific candidates
            WHEN filer_type_cd IN ('SPAC', 'ASIFSPAC', 'JSPC', 'SCPC') THEN 'A'

            -- Unauthorized PACs
            WHEN filer_type_cd IN ('GPAC', 'MPAC') THEN 'U'

            -- Party-related, not authorized by candidate
            WHEN filer_type_cd IN ('PTYCORP', 'CEC', 'MCEC') THEN 'U'

            -- Independent expenditure reports
            WHEN filer_type_cd = 'DCE' THEN 'U'

            -- Leadership PAC / legislative caucus
            WHEN filer_type_cd = 'LEG' THEN 'D'
        END AS committee_designation,
        filer_name AS "name",
        filer_street_addr1 AS address1,
        filer_street_addr2 AS address2,
        filer_street_city AS city,
        filer_street_state_cd AS "state",
        filer_street_postal_code AS zip_code,
        NULL AS affiliation,
        cta_seek_office_district AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE row_num = 1
)

SELECT
    source,
    filer_ident,
    committee_id,
    candidate_id,
    candidate_name,
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

{{
    config(
        materialized='table',
        tags=["rhode_island", "committee", "master"]
    )
}}

WITH contribution_filter AS (
    SELECT 
        "OrganizationName",
        "Address",
        "CityStZip",
        "EmployerName",
        "ReceiptDate",
        "Amount",
        ROW_NUMBER() OVER (
            PARTITION BY "OrganizationName" 
            ORDER BY "ReceiptDate" DESC
        ) AS row_seq
    FROM {{ source('ri', 'ri_contributions_landing') }}
    WHERE "TransType" = 'Contribution'
        AND "OrganizationName" IS NOT NULL
        AND TRIM("OrganizationName") <> ''
),

committee_aggregate AS (
    SELECT
        'RI' AS source,
        'RI_' || {{ dbt_utils.generate_surrogate_key(['"OrganizationName"']) }} AS committee_id,
        "OrganizationName" AS name,
        'RI' AS state,  -- Hardcode state value
        MODE() WITHIN GROUP (ORDER BY TRIM(SPLIT_PART("CityStZip", ',', 1))) AS city
    FROM contribution_filter
    GROUP BY "OrganizationName"
),

address_parsing AS (
    SELECT
        "OrganizationName",
        TRIM(SPLIT_PART("Address", ',', 1)) AS address1,
        CASE 
            WHEN SPLIT_PART("Address", ',', 2) IS NOT NULL 
            THEN TRIM(SPLIT_PART("Address", ',', 2))
            ELSE NULL 
        END AS address2
    FROM contribution_filter
    WHERE row_seq = 1
)

SELECT
    ca.source,
    ca.committee_id,
    NULL AS committee_designation,
    ca.name,
    ap.address1,
    ap.address2,
    ca.city,
    ca.state,
    NULL AS zip_code,
    NULL AS affiliation,
    NULL AS district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM committee_aggregate ca
LEFT JOIN address_parsing ap
    ON ca.name = ap."OrganizationName"
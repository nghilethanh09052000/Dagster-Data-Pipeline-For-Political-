{{
    config(
        materialized='table',
        tags=["new_york", "contributions", "master"]
    )
}}

{%- set table_names = [
    'ny_state_candidate_landing',
    'ny_county_candidate_landing',
    'ny_state_committee_landing',
    'ny_county_committee_landing',
] %}


WITH source_data AS (
    {%- for table in table_names %}
        SELECT
            '{{ table }}' AS source_table,
            "FILER_ID" AS filer_id,
            "TRANS_NUMBER" AS trans_number,
            "SCHED_DATE" AS sched_date,
            "FLNG_ENT_NAME" AS flng_ent_name,
            "FLNG_ENT_FIRST_NAME" AS flng_ent_first_name,
            "FLNG_ENT_MIDDLE_NAME" AS flng_ent_middle_name,
            "FLNG_ENT_LAST_NAME" AS flng_ent_last_name,
            "FLNG_ENT_ADD1" AS flng_ent_add1,
            "FLNG_ENT_CITY" AS flng_ent_city,
            "FLNG_ENT_STATE" AS flng_ent_state,
            "FLNG_ENT_ZIP" AS flng_ent_zip,
            "ORG_AMT" AS org_amt,
            ROW_NUMBER() OVER (
                PARTITION BY "FILER_ID", "TRANS_NUMBER" 
                ORDER BY "SCHED_DATE", "ORG_AMT"
            ) AS row_num
        FROM {{ source('ny', table) }}
        WHERE
            -- Ref: https://elections.ny.gov/system/files/documents/2023/08/campaignfinancehandbook.pdf
            -- Schedule A: Monetary Contributions Received from Individuals & Partnerships
            -- Schedule B: Monetary Contributions Received from Corporations
            -- Schedule C: Monetary Contributions Received from All Other Entities
            -- Schedule D: In-Kind (Non-Monetary) Contributions Received
            -- Schedule E: Other Receipts Received
          "FILING_SCHED_ABBREV" = 'A' AND
          "R_AMEND" = 'N' AND
          "CNTRBR_TYPE_DESC" = 'Individual' AND
          "ORG_AMT" IS NOT NULL AND
          TRIM("ORG_AMT") != '' AND
          "SCHED_DATE" IS NOT NULL AND
          TRIM("SCHED_DATE") != ''
        {%- if not loop.last %}
            UNION ALL
        {%- endif %}
    {%- endfor %}
),

cleaned_data AS (
    SELECT
        'NY' AS source,
        'NY_' ||
        CASE
            WHEN source_table = 'ny_state_candidate_landing' THEN 'SC_'
            WHEN source_table = 'ny_county_candidate_landing' THEN 'CC_'
            WHEN source_table = 'ny_state_committee_landing' THEN 'SCO_'
            WHEN source_table = 'ny_county_committee_landing' THEN 'CCO_'
        END ||
        COALESCE(filer_id, '') || '_' ||
        COALESCE(trans_number, '') || '_' || row_num::text AS source_id,
        'NY_' || filer_id AS committee_id,
        CASE
            WHEN flng_ent_name IS NOT NULL AND TRIM(flng_ent_name) != '' THEN flng_ent_name
            ELSE flng_ent_first_name || ' ' || CASE
                WHEN flng_ent_middle_name IS NOT NULL AND TRIM(flng_ent_middle_name) != ''
                THEN flng_ent_middle_name || ' '
                ELSE ''
              END || flng_ent_last_name
        END AS "name",
        flng_ent_city AS city,
        flng_ent_state AS "state",
        flng_ent_zip AS zip_code,
        NULL AS employer,
        NULL AS occupation,
        org_amt::decimal(10, 2) AS amount,
        sched_date::timestamp AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
)

SELECT
    source,
    source_id,
    committee_id,
    name,
    city,
    state,
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM cleaned_data

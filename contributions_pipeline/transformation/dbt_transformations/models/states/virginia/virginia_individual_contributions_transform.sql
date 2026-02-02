{{
    config(
      materialized='table',
      tags=["virginia", "contributions", "master"]
    )
}}

WITH source_data AS (

    -- Start Schedule A

    SELECT
        'VA' AS "source",
        'VA_A_PAC_' || {{ dbt_utils.generate_surrogate_key(['report_code', 'entity_name', 'trans_amount', 'trans_date', 'trans_agg_to_date']) }} AS source_id,
        'VA_' || committee_code AS committee_id,
        entity_name AS "name",
        entity_city AS city,
        entity_state AS "state",
        entity_zip AS zip_code,
        entity_employer AS employer,
        entity_occupation AS occupation,
        trans_amount::decimal AS amount,
        TO_TIMESTAMP(trans_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY report_code, entity_name, trans_amount, trans_date, trans_agg_to_date
        ) AS row_num
    FROM {{ source('va', 'schedulea_pac_old_landing') }}
    WHERE
        (
            amended_count IS NULL
            OR TRIM(amended_count) = ''
            OR TRIM(amended_count) = '0'
        )
        AND trans_date IS NOT NULL
        AND trans_date <> ''
        AND trans_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_A_PAC_' || {{ dbt_utils.generate_surrogate_key(['report_code', 'entity_name', 'trans_amnt', 'trans_date', 'trans_agg_to_date']) }} AS source_id,
        'VA_' || committee_code AS committee_id,
        entity_name AS "name",
        entity_city AS city,
        entity_state AS "state",
        entity_zip AS zip_code,
        entity_employer AS employer,
        entity_occupation AS occupation,
        trans_amnt::decimal AS amount,
        CASE
            WHEN trans_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                THEN TO_TIMESTAMP(trans_date, 'MM/DD/YYYY')
            WHEN trans_date ~ '^\d{1,2}-[A-Za-z]{3}$'
                THEN
                    TO_TIMESTAMP(
                        trans_date || '-' || report_year,
                        'DD-Mon-YYYY'
                    )
            WHEN trans_date ~ '^\d+$'
                -- Excel date system https://support.microsoft.com/en-us/office/date-systems-in-excel-e7fe7167-48a9-4b96-bb53-5612a800b487
                THEN '1900-01-01'::timestamp + (trans_date || ' days')::interval
        END AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY report_code, entity_name, trans_amnt, trans_date, trans_agg_to_date
        ) AS row_num
    FROM {{ source('va', 'schedulea_pac_transitional_landing') }}
    WHERE
        (amended_count IS NULL OR TRIM(amended_count) = '0')
        AND trans_date IS NOT NULL
        AND trans_date <> ''
        AND (
            trans_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            OR trans_date ~ '^\d{1,2}-[A-Za-z]{3}$'
            OR trans_date ~ '^\d+$'
        )

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_A_' || sa.reportuid || '_' || sa.scheduleaid AS source_id,
        'VA_' || rl.committeecode AS committee_id,
        TRIM(
            COALESCE(NULLIF(sa.firstname, '') || ' ', '')
            || COALESCE(NULLIF(sa.middlename, '') || ' ', '')
            || sa.lastorcompanyname
            || CASE
                WHEN
                    sa.suffix IS NOT NULL AND TRIM(sa.suffix) <> ''
                    THEN ' ' || sa.suffix
                ELSE ''
            END
        ) AS "name",
        sa.city,
        sa.statecode AS "state",
        sa.zipcode AS zip_code,
        sa.nameofemployer AS employer,
        sa.occupationortypeofbusiness AS occupation,
        sa.amount::decimal AS amount,
        TO_TIMESTAMP(sa.transactiondate, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY sa.reportuid, sa.scheduleaid
        ) AS row_num
    FROM {{ source('va', 'schedulea_new_landing') }} AS sa
    INNER JOIN {{ source('va', 'report_landing') }} AS rl
        ON sa.reportuid = rl.reportuid
    WHERE
        sa.isindividual = 'True'
        AND sa.transactiondate IS NOT NULL
        AND sa.transactiondate <> ''
        AND sa.transactiondate ~ '^\d{1,2}/\d{1,2}/\d{4}$'

    UNION ALL

    -- Start Schedule B

    SELECT
        'VA' AS "source",
        'VA_B_PAC_' || {{ dbt_utils.generate_surrogate_key(['report_code', 'entity_name', 'trans_amount', 'trans_date', 'trans_agg_to_date']) }} AS source_id,
        'VA_' || committee_code AS committee_id,
        entity_name AS "name",
        entity_city AS city,
        entity_state AS "state",
        entity_zip AS zip_code,
        entity_employer AS employer,
        entity_occupation AS occupation,
        trans_amount::decimal AS amount,
        TO_TIMESTAMP(trans_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY report_code, entity_name, trans_amount, trans_date, trans_agg_to_date
        ) AS row_num
    FROM {{ source('va', 'scheduleb_pac_old_landing') }}
    WHERE
        (
            amended_count IS NULL
            OR TRIM(amended_count) = ''
            OR TRIM(amended_count) = '0'
        )
        AND trans_date IS NOT NULL
        AND trans_date <> ''
        AND trans_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_B_OLD_' || {{ dbt_utils.generate_surrogate_key(['report_code', 'entity_name', 'trans_date', 'trans_amount', 'trans_agg_to_date']) }} AS source_id,
        'VA_' || committee_code AS committee_id,
        entity_name AS "name",
        entity_city AS city,
        entity_state AS "state",
        entity_zip AS zip_code,
        entity_employer AS employer,
        entity_occupation AS occupation,
        trans_amount::decimal AS amount,
        TO_TIMESTAMP(trans_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY report_code, entity_name, trans_amount, trans_date, trans_agg_to_date
        ) AS row_num
    FROM {{ source('va', 'scheduleb_old_landing') }}
    WHERE
        (
            amended_count IS NULL
            OR TRIM(amended_count) = ''
            OR TRIM(amended_count) = '0'
        )
        AND trans_date IS NOT NULL
        AND trans_date <> ''
        AND trans_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_B_' || sb.reportuid || '_' || sb.schedulebid AS source_id,
        'VA_' || rl.committeecode AS committee_id,
        TRIM(
            COALESCE(NULLIF(sb.firstname, '') || ' ', '')
            || COALESCE(NULLIF(sb.middlename, '') || ' ', '')
            || sb.lastorcompanyname
            || CASE
                WHEN
                    sb.suffix IS NOT NULL AND TRIM(sb.suffix) <> ''
                    THEN ' ' || sb.suffix
                ELSE ''
            END
        ) AS "name",
        sb.city,
        sb.statecode AS "state",
        sb.zipcode AS zip_code,
        sb.nameofemployer AS employer,
        sb.occupationortypeofbusiness AS occupation,
        sb.amount::decimal AS amount,
        CASE
            WHEN sb.transactiondate ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                THEN TO_TIMESTAMP(sb.transactiondate, 'MM/DD/YYYY')
            WHEN sb.transactiondate ~ '^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$'
                THEN sb.transactiondate::timestamp
        END AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY sb.reportuid, sb.schedulebid
        ) AS row_num
    FROM {{ source('va', 'scheduleb_new_landing') }} AS sb
    INNER JOIN {{ source('va', 'report_landing') }} AS rl
        ON sb.reportuid = rl.reportuid
    WHERE
        sb.isindividual = 'True'
        AND sb.transactiondate IS NOT NULL
        AND sb.transactiondate <> ''
        AND (
            sb.transactiondate ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            OR sb.transactiondate ~ '^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$'
        )
),

joined_data AS (
    SELECT
        sd.*,
        cm.candidate_id,
        cm.candidate_name
    FROM source_data AS sd
    LEFT JOIN {{ ref('virginia_committee_master_transform') }} AS cm
        ON sd.committee_id = cm.committee_id
    WHERE
        sd.row_num = 1
        -- Smoke test committee
        AND sd.committee_id <> 'VA_CC-21-00399'
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
FROM joined_data

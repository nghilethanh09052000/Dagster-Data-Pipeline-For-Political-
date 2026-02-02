{{ config(
    materialized='table',
    tags=["ohio", "individual_contributions"]
) }}

WITH source_data AS (
    SELECT
        'OH_COM_' || "MASTER_KEY" AS committee_id,
        "COM_NAME" AS com_name,
        "PAC_REG_NO" AS pac_reg_no,
        "MASTER_KEY" AS master_key,
        "RPT_YEAR" AS rpt_year,
        "REPORT_KEY" AS report_key,
        "REPORT_DESCRIPTION" AS report_description,
        "SHORT_DESCRIPTION" AS short_description,
        "FIRST_NAME" AS first_name,
        "MIDDLE_NAME" AS middle_name,
        "LAST_NAME" AS last_name,
        "SUFFIX_NAME" AS suffix_name,
        "NON_INDIVIDUAL" AS non_individual,
        "ADDRESS" AS address,
        "CITY" AS city,
        "STATE" AS "state",
        "ZIP" AS zip,
        "FILE_DATE" AS file_date,
        "AMOUNT" AS amount,
        "EVENT_DATE" AS event_date,
        "EMP_OCCUPATION" AS emp_occupation,
        "INKIND_DESCRIPTION" AS inkind_description,
        "OTHER_INCOME_TYPE" AS other_income_type,
        "RCV_EVENT" AS rcv_event,
        NULL AS candidate_name,
        NULL AS candidate_party
    FROM {{ source('oh', 'oh_committee_contribution_landing') }}
    WHERE
        "AMOUNT" <> ''
        AND "FILE_DATE" <> ''

    UNION ALL

    SELECT
        'OH_CAN_' || "MASTER_KEY" AS committee_id,
        "COM_NAME" AS com_name,
        "PAC_REG_NO" AS pac_reg_no,
        "MASTER_KEY" AS master_key,
        "RPT_YEAR" AS rpt_year,
        "REPORT_KEY" AS report_key,
        "REPORT_DESCRIPTION" AS report_description,
        "SHORT_DESCRIPTION" AS short_description,
        "FIRST_NAME" AS first_name,
        "MIDDLE_NAME" AS middle_name,
        "LAST_NAME" AS last_name,
        "SUFFIX_NAME" AS suffix_name,
        "NON_INDIVIDUAL" AS non_individual,
        "ADDRESS" AS address,
        "CITY" AS city,
        "STATE" AS "state",
        "ZIP" AS zip,
        "FILE_DATE" AS file_date,
        "AMOUNT" AS amount,
        "EVENT_DATE" AS event_date,
        "EMP_OCCUPATION" AS emp_occupation,
        "INKIND_DESCRIPTION" AS inkind_description,
        "OTHER_INCOME_TYPE" AS other_income_type,
        "RCV_EVENT" AS rcv_event,
        trim(
            coalesce("CANDIDATE_FIRST_NAME", '')
            || ' '
            || coalesce("CANDIDATE_LAST_NAME", '')
        ) AS candidate_name,
        "PARTY" AS candidate_party
    FROM {{ source('oh', 'oh_candidate_contribution_landing') }}
    WHERE
        "AMOUNT" <> ''
        AND "FILE_DATE" <> ''
),

transformed AS (
    SELECT
        -- Static source identifier
        'OH' AS "source",

        -- Unique surrogate key with all columns from source_data included
        'OH_' || {{ dbt_utils.generate_surrogate_key([
            "com_name",
            "pac_reg_no",
            "master_key",
            "rpt_year",
            "report_key",
            "report_description",
            "short_description",
            "first_name",
            "middle_name",
            "last_name",
            "suffix_name",
            "non_individual",
            "address",
            "city",
            "state",
            "zip",
            "file_date",
            "amount",
            "event_date",
            "emp_occupation",
            "inkind_description",
            "other_income_type",
            "rcv_event"
        ]) }} AS source_id,

        -- Committee ID from master_key
        committee_id,

        CASE
            WHEN candidate_name IS NOT NULL AND candidate_party IS NOT NULL THEN
                'OH_'
                || {{ dbt_utils.generate_surrogate_key([
                      'candidate_name',
                      'candidate_party'
                   ]) }}
        END AS candidate_id,
        candidate_name,

        -- Contributor full name (capitalized)
        initcap(trim(
            concat_ws(
                ' ',
                nullif(first_name, ''),
                nullif(middle_name, ''),
                nullif(last_name, ''),
                nullif(suffix_name, '')
            )
        )) AS "name",

        nullif(trim(city), '') AS city,
        nullif(trim(state), '') AS "state",

        zip AS zip_code,

        -- Employer and occupation (split by '/')
        trim(split_part(emp_occupation, '/', 1)) AS employer,
        trim(split_part(emp_occupation, '/', 2)) AS occupation,

        -- Amount parsed to float
        replace(trim(amount), ',', '')::FLOAT AS amount,

        -- Contribution date from file_date cast to timestamp
        file_date::TIMESTAMP AS contribution_datetime,

        current_timestamp AS insert_datetime

    FROM source_data
    WHERE
        trim(first_name) IS NOT NULL
        AND trim(last_name) IS NOT NULL
        AND length(zip) > 0
)

SELECT DISTINCT * FROM transformed

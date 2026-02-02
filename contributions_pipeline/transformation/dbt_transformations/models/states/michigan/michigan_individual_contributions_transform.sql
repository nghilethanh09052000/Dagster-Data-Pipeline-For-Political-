{{
    config(
      materialized='table',
      tags=["michigan", "contributions", "master"]
    )
}}

WITH legacy_source_data AS (
    SELECT
        doc_seq_no,
        contribution_id,
        cont_detail_id,
        cfr_com_id,
        (
            CASE
                WHEN f_name != '' AND TRIM(f_name) != '' THEN f_name ELSE ''
            END
            || CASE
                WHEN l_name != '' AND TRIM(l_name) != '' THEN l_name ELSE ''
            END
        ) AS contrib_name,
        address,
        city,
        state,
        zip,
        employer,
        occupation,
        amount,
        received_date
    FROM {{ source('mi', 'mi_contributions_landing') }}
    WHERE
        amount != ''
        AND TRIM(amount) != ''
        AND amount ~ '^[0-9]+(\.[0-9]+)?$'
),

mitn_source_data AS (
    SELECT
        data_id,
        committee_id,
        committee_name,
        contributor_name,
        (
            REGEXP_MATCHES(
                contributor_adddress,
                ',\s*([^,]+),\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?'
            )
        )[1] AS city,
        (
            REGEXP_MATCHES(
                contributor_adddress,
                ',\s*[^,]+,\s*([A-Z]{2})\s*\d{5}(?:-\d{4})?'
            )
        )[1] AS "state",
        (REGEXP_MATCHES(contributor_adddress, '\b\d{5}(?:-\d{4})?\b'))[1]
            AS zip,
        contributor_occupation,
        contributor_employer,
        contribution_date,
        contribution_amount,
        ROW_NUMBER() OVER (PARTITION BY data_id) AS row_num
    FROM {{ source('mi', 'mi_mitn_contributions_landing') }}
    WHERE
        TRIM(schedule_type) = 'Direct Contributions'
        AND TRIM(contributor_adddress) != ''
),

transformed_data AS (
    SELECT
        'MI' AS "source",
        'MI'
        || '_'
        || doc_seq_no
        || '_'
        || contribution_id
        || '_'
        || cont_detail_id AS source_id,
        'MI_' || cfr_com_id AS committee_id,
        contrib_name AS "name",
        city,
        state,
        zip AS zip_code,
        employer,
        occupation,
        amount::decimal AS amount,
        TO_TIMESTAMP(received_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM legacy_source_data

    UNION ALL

    SELECT
        'MI' AS "source",
        'MI'
        || '_'
        || data_id AS source_id,
        'MI_' || committee_id AS committee_id,
        contributor_name AS "name",
        city,
        state,
        zip AS zip_code,
        contributor_employer AS employer,
        contributor_occupation AS occupation,
        {{ clean_dolar_sign_amount('contribution_amount') }} AS amount,
        TO_TIMESTAMP(contribution_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM mitn_source_data
    WHERE row_num = 1
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
FROM transformed_data

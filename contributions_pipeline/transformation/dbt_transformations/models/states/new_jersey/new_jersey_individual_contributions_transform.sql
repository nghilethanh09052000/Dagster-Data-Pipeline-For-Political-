{{
    config(
      materialized='table',
      tags=["new_jersey", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        cont_lname,
        cont_fname,
        cont_mname,
        cont_suffix,
        (
            CASE WHEN cont_suffix IS NOT NULL AND TRIM(cont_suffix) != '' THEN (' ' || cont_suffix) ELSE '' END
            || cont_fname
            || CASE WHEN cont_mname IS NOT NULL AND TRIM(cont_mname) != '' THEN (' ' || cont_mname) ELSE '' END
            || CASE WHEN cont_lname IS NOT NULL AND TRIM(cont_lname) != '' THEN (' ' || cont_lname) ELSE '' END
        ) AS "cont_name",
        cont_street1,
        cont_street2,
        cont_city,
        cont_state,
        cont_zip,
        TRANSLATE(cont_amt, ',', '')::decimal AS cont_amt,
        to_timestamp(cont_date, 'MM/DD/YYYY') AS cont_date,
        occupation,
        emp_name,
        rec_suffix,
        rec_fname,
        rec_mname,
        rec_lname,
        rec_non_ind_name,
        rec_non_ind_name2,
        party,
        "location"
    FROM {{ source('nj', 'nj_contributions_landing') }}
    WHERE
        cont_type = 'INDIVIDUAL'
        AND cont_fname IS NOT NULL
        AND TRIM(cont_fname) != ''
        AND cont_amt IS NOT NULL
        AND TRIM(cont_amt) != ''
        AND TRANSLATE(cont_amt, ',', '')::decimal > 0
),

cont_to_ind AS (
    SELECT
        *,
        (
            CASE WHEN rec_suffix IS NOT NULL AND TRIM(rec_suffix) != '' THEN (' ' || rec_suffix) ELSE '' END
            || rec_fname
            || CASE WHEN rec_mname IS NOT NULL AND TRIM(rec_mname) != '' THEN (' ' || rec_mname) ELSE '' END
            || CASE WHEN rec_lname IS NOT NULL AND TRIM(rec_lname) != '' THEN (' ' || rec_lname) ELSE '' END
        ) AS "rec_name",
        ROW_NUMBER() OVER (
            PARTITION BY cont_name, cont_amt, cont_date, cont_city, cont_state, cont_zip
        ) AS row_num
    FROM source_data
    WHERE
        rec_fname IS NOT NULL
        AND TRIM(rec_fname) != ''
),

cont_to_pac AS (
    SELECT
        *,
        (
            rec_non_ind_name
            || CASE WHEN rec_non_ind_name2 IS NOT NULL AND TRIM(rec_non_ind_name2) != '' THEN (' ' || rec_non_ind_name2) ELSE '' END
        ) AS "rec_name",
        ROW_NUMBER() OVER (
            PARTITION BY cont_name, cont_amt, cont_date, cont_city, cont_state, cont_zip
        ) AS row_num

    FROM source_data
    WHERE
        rec_non_ind_name IS NOT NULL
        AND TRIM(rec_non_ind_name) != ''
),

cont_to_ind_transformed AS (
    SELECT
        'NJ' AS "source",
        'NJ_IND_' || {{ dbt_utils.generate_surrogate_key(['cont_name', 'cont_amt', 'cont_date', 'cont_city', 'cont_state', 'cont_zip']) }} AS source_id,
        'NJ_' || {{ dbt_utils.generate_surrogate_key(['rec_name', 'party', 'location']) }} AS committee_id,
        cont_name as "name",
        cont_city AS city,
        cont_state AS "state",
        cont_zip AS zip_code,
        emp_name AS employer,
        occupation,
        cont_amt AS amount,
        cont_date AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM cont_to_ind
    WHERE row_num = 1
),

cont_to_pac_transformed AS (
    SELECT
        'NJ' AS "source",
        'NJ_PAC_' || {{ dbt_utils.generate_surrogate_key(['cont_name', 'cont_amt', 'cont_date', 'cont_city', 'cont_state', 'cont_zip']) }} AS source_id,
        'NJ_' || {{ dbt_utils.generate_surrogate_key(['rec_name', 'party', 'location']) }} AS committee_id,
        cont_name as "name",
        cont_city AS city,
        cont_state AS "state",
        cont_zip AS zip_code,
        emp_name AS employer,
        occupation,
        cont_amt AS amount,
        cont_date AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM cont_to_pac
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
FROM cont_to_ind_transformed

UNION ALL

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
FROM cont_to_pac_transformed

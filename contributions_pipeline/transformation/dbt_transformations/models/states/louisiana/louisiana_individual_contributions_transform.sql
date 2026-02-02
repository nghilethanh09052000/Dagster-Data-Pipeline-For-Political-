{{
    config(
        materialized='table',
        tags=["louisiana", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        filer_first_name,
        filer_last_name,
        filer_first_name ||
            CASE
                WHEN filer_last_name IS NOT NULL AND TRIM(filer_last_name) != ''
                THEN ' ' || filer_last_name
                ELSE ''
            END AS filer_full_name,
        contributor_name,
        contributor_city,
        contributor_state,
        contributor_zip,
        contribution_date,
        contribution_amt,
        ROW_NUMBER() OVER (
            PARTITION BY contributor_name, contribution_date, contribution_amt
            ORDER BY contributor_name
        ) AS row_num
    FROM {{ source('la', 'la_contributions_landing') }}
    WHERE
        contribution_type = 'CONTRIB'
        AND contributor_type_code = 'IND'
        AND contribution_amt IS NOT NULL
        AND TRIM(contribution_amt) != ''
        AND contribution_date IS NOT NULL
        AND TRIM(contribution_date) != ''
),

transformed_data AS (
    SELECT
        'LA' AS "source",
        'LA_' || {{ dbt_utils.generate_surrogate_key(['contributor_name', 'contribution_date', 'contribution_amt', 'filer_full_name']) }} AS source_id,
        {{ generate_committee_id('LA_', 'filer_full_name') }} AS committee_id,
        contributor_name AS "name",
        contributor_city AS city,
        contributor_state AS "state",
        contributor_zip AS zip_code,
        NULL AS employer,
        NULL AS occupation,
        {{ clean_dolar_sign_amount('contribution_amt') }} AS amount,
        to_timestamp(contribution_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_data
    WHERE row_num = 1
)

SELECT
    "source",
    source_id,
    committee_id,
    "name",
    city,
    "state",
    zip_code,
    employer,
    occupation,
    amount,
    contribution_datetime,
    insert_datetime
FROM transformed_data

{{
    config(
      materialized='table',
      tags=["tennessee", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        amount,
        "date",
        recipient_name,
        contributor_name,
        contributor_address,
        TRIM(SPLIT_PART(contributor_address, ',', 2)) AS city,
        TRIM(SPLIT_PART(contributor_address, ',', 3)) AS "state",
        TRIM(SPLIT_PART(contributor_address, ',', 4)) AS zip_code,
        contributor_occupation,
        contributor_employer
    FROM {{ source('tn', 'tn_contributions_landing') }}
    WHERE
        adj = 'N'
        AND "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        AND amount IS NOT NULL
        -- Check if the row have at least 1 number inside of it
        AND amount ~ '[0-9]'
),

transformed_data AS (
    SELECT
        'TN' AS "source",
        'TN_' || {{ dbt_utils.generate_surrogate_key([
            'contributor_name',
            'recipient_name',
            'amount',
            '"date"',
            'contributor_address'
        ]) }} AS source_id,
        'TN_' || {{ dbt_utils.generate_surrogate_key(['recipient_name']) }} AS committee_id,
        contributor_name AS "name",
        city,
        "state",
        zip_code,
        contributor_employer AS employer,
        contributor_occupation AS occupation,
        {{ clean_dolar_sign_amount('amount') }} AS amount,
        {{ safe_parse_date('"date"') }} AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY contributor_name, recipient_name, amount, "date", contributor_address
        ) AS row_num
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
FROM transformed_data
WHERE
    row_num = 1
    -- Need to check as with safe parse date macro
    -- it will return NULL on invalid date
    AND contribution_datetime IS NOT NULL

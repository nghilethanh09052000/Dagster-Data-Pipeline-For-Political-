{{
    config(
        materialized='table',
        tags=["connecticut", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'CT' as source,
        'CT_' || {{ dbt_utils.generate_surrogate_key(
            [
                'receipt_id', 
                'transaction_date', 
                'received_from', 
                'amount'
            ]) 
        }} as source_id,
        'CT_' || receipt_id as committee_id,
        received_from as name,
        city,
        state,
        NULL as zip_code,
        employer,
        occupation,
        REGEXP_REPLACE(
            CASE
                WHEN amount ~ '^-?\d+(\.\d+)?$' THEN amount
                WHEN amount ~ '^\d+(\.\d+)?-$' THEN '-' || REGEXP_REPLACE(amount, '-', '', 'g')
                ELSE NULL
            END,
            ',', '',
            'g'
        )::decimal(10,2) AS amount,
        CASE 
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN transaction_date::timestamp
            WHEN transaction_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                AND split_part(transaction_date, '/', 1)::int BETWEEN 1 AND 12
                AND split_part(transaction_date, '/', 2)::int BETWEEN 1 AND 31
                THEN to_timestamp(transaction_date, 'MM/DD/YYYY')
            WHEN file_to_state_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                AND split_part(file_to_state_date, '/', 3) ~ '^\d{4}$'
                THEN to_timestamp(split_part(file_to_state_date, '/', 3) || '-01-01', 'YYYY-MM-DD')
            ELSE NULL
        END AS contribution_datetime,
        CURRENT_TIMESTAMP as insert_datetime
    FROM {{ source('ct', 'ct_contributions_landing') }}
    WHERE transaction_date IS NOT NULL 
      AND transaction_date <> ''
      AND (
          transaction_date ~ '^\d{4}-\d{2}-\d{2}$' OR 
          transaction_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      )
      
)

, final AS(
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
    FROM source_data
    WHERE 
        amount IS NOT NULL 
)
SELECT DISTINCT * FROM final
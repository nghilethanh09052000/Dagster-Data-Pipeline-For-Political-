{{
    config(
        materialized='incremental',
        unique_key='source_id',
        tags=["arkansas", "contributions", "master"],
        incremental_strategy="delete+insert",
        post_hook="CREATE INDEX IF NOT EXISTS idx_contributions_year ON {{ this }} (date_part('year', contribution_datetime))"
    )
}}

WITH raw_data AS (
    SELECT
        'AR' AS source,
        'AR_' || guid AS source_id,
        'AR_' || filer_registration_guid AS committee_id,
        source_name AS name,
        NULL AS city,
        NULL AS state,
        REGEXP_SUBSTR(
            TRIM(
                CASE
                    WHEN source_address ILIKE '%,%'
                        THEN
                            SPLIT_PART(
                                source_address,
                                ',',
                                ARRAY_LENGTH(
                                    STRING_TO_ARRAY(source_address, ','), 1
                                )
                            )
                    ELSE NULL
                END
            ),
            '\d{5}'
        ) AS zip_code,
        employer_name AS employer,
        occupation,
        transaction_amount::decimal(10, 2) AS amount,
        transaction_date::timestamp AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (PARTITION BY guid) AS row_num
    FROM {{ source('ar', 'ar_contributions_landing') }}
),

filtered_data AS (
    SELECT *
    FROM raw_data AS raw_data
    WHERE
        raw_data.row_num = 1
        {% if is_incremental() %}
            AND DATE_PART('year', raw_data.contribution_datetime) >= (
                SELECT MAX(DATE_PART('year', curr.contribution_datetime))
                FROM {{ this }} AS curr
            )
        {% endif %}
)

SELECT * FROM filtered_data

{{ config(
    materialized='table',
    tags=["oklahoma", "contributions", "master"]
) }}


{% set surrogate_key_columns = [
    'receipt_id',
    'org_id',
    'receipt_type',
    'receipt_date',
    'receipt_amount',
    'last_name',
    'first_name',
    'middle_name',
    'suffix',
    'address_1',
    'address_2',
    'city',
    'state',
    'zip',
    'filed_date',
    'committee_type',
    'committee_name',
    'candidate_name',
    'amended',
    'employer',
    'occupation'
] %}

WITH source_data AS (
    SELECT DISTINCT
        {% for col in surrogate_key_columns %}
            {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source('ok', 'ok_contributions_loans_landing') }}
    WHERE 
        org_id is not null and org_id <> ''
        AND receipt_amount is not null and receipt_amount <> ''
),

cleaned AS (
    SELECT
        'OK' AS source,

        'OK_' || {{ dbt_utils.generate_surrogate_key(surrogate_key_columns) }} AS source_id,

        'OK_' || org_id AS committee_id,

        TRIM(
            CONCAT_WS(' ',
                NULLIF(first_name, ''),
                NULLIF(middle_name, ''),
                NULLIF(last_name, ''),
                NULLIF(suffix, '')
            )
        ) AS name,

        city,
        state,
        zip AS zip_code,
        employer,
        occupation,
        receipt_amount::decimal(14,2) AS amount,
        to_timestamp(receipt_date, 'MM/DD/YYYY') AS contribution_datetime,

        CURRENT_TIMESTAMP AS insert_datetime

    FROM source_data
)

SELECT * FROM cleaned

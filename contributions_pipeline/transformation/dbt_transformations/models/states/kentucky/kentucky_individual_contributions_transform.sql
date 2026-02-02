{{
    config(
        materialized='table',
        tags=["kentucky", "contributions", "master"]
    )
}}

{% set all_columns = [
    'to_organization', 'from_organization_name',
    'contributor_last_name', 'contributor_first_name',
    'recipient_last_name', 'recipient_first_name',
    'office_sought', 'location',
    'election_date', 'election_type', 'exemption_status', 'other_text',
    'address1', 'address2', 'city', 'state', 'zip',
    'amount', 'contribution_type', 'contribution_mode',
    'occupation', 'other_occupation', 'employer',
    'spouse_prefix', 'spouse_last_name', 'spouse_first_name',
    'spouse_middle_initial', 'spouse_suffix',
    'spouse_occupation', 'spouse_employer',
    'number_of_contributors', 'inkind_description',
    'receipt_date', 'statement_type'
] %}

{% set surrogate_key_fields = all_columns %}

WITH source_data AS (

    SELECT DISTINCT
        {{ all_columns | join(',\n        ') }}
    FROM {{ source('ky', 'ky_contributions_landing') }}
    WHERE
        to_organization IS NOT NULL AND to_organization <> ''
        AND amount IS NOT NULL AND amount <> ''
        AND receipt_date IS NOT NULL AND receipt_date <> ''

)

, transformed AS (

    SELECT
        'KY' AS source,
        'KY_' || {{ dbt_utils.generate_surrogate_key(surrogate_key_fields) }} AS source_id,
        {{ generate_committee_id('KY_', 'to_organization') }} AS committee_id,

        CONCAT(contributor_first_name, ' ', contributor_last_name) AS name,
        city,
        state,
        zip AS zip_code,
        employer,
        occupation,
        amount::NUMERIC AS amount,
        receipt_date::TIMESTAMP AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime

    FROM source_data
    where contribution_type = 'INDIVIDUAL'

)

SELECT * FROM transformed

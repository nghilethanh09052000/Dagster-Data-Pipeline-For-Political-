{{
    config(
      materialized='table',
      tags=["florida", "contributions", "master"]
    )
}}

WITH source_data AS (
    SELECT
        "Candidate/Committee" AS committee_name,
        "Date" AS contrib_date,
        "Amount" AS amount,
        "Type" AS contrib_type,
        "Contributor_Name" AS contrib_name,
        "Address" AS address,
        "City_State_Zip" AS city_state_zip,
        "Occupation" AS contrib_occupation
    FROM {{ source('fl', 'fl_contributions_landing') }}
    WHERE
        "Candidate/Committee" != 'LASTNAME FIRSTNAME MIDDLENAME'
        AND ("Amount" IS NOT NULL AND TRIM("Amount") != '')
        AND ("Candidate/Committee" IS NOT NULL AND TRIM("Candidate/Committee") != '')
),

parsed_locations AS (
    SELECT
        *,
        -- Extract city: everything before the comma, removing non-alphanumeric chars
        NULLIF(
            REGEXP_REPLACE(
                CASE
                    WHEN POSITION(',' IN city_state_zip) > 0
                        THEN TRIM(SUBSTRING(city_state_zip FROM 1 FOR POSITION(',' IN city_state_zip) - 1))
                END,
                '[^a-zA-Z0-9 ]', '', 'g'
            ),
            ''
        ) AS city,
        -- Extract state: look for 2 characters after a comma and convert to uppercase
        UPPER(
            NULLIF(
                REGEXP_REPLACE(
                    SUBSTRING(city_state_zip FROM ',\s*([A-Za-z]{2})($|\s|\d)'),
                    ',[^A-Za-z]*([A-Za-z]{2}).*', '\1'
                ),
                ''
            )
        ) AS state,
        -- Extract zip: look for 5 digits with better boundary detection
        NULLIF(
            REGEXP_REPLACE(
                SUBSTRING(city_state_zip FROM '(\d{5})($|\s)'),
                '.*?(\d{5})($|\s).*', '\1'
            ),
            ''
        ) AS zip_code
    FROM source_data
),

cleaned_data AS (
    SELECT
        'FL' AS source,
        'FL_' || {{ dbt_utils.generate_surrogate_key([
            'contrib_name', 
            'committee_name', 
            'contrib_date', 
            'amount', 
            'address', 
            'city', 
            'state', 
            'zip_code',
            'contrib_occupation'
        ]) }} AS source_id,
        {{ generate_committee_id('FL_', 'committee_name') }} AS committee_id,
        contrib_name AS name,
        city,
        state,
        zip_code,
        NULL AS employer,
        contrib_occupation AS occupation,
        amount::decimal AS amount,
        TO_TIMESTAMP(contrib_date, 'MM/DD/YYYY') AS contribution_datetime,
        CURRENT_TIMESTAMP AS insert_datetime,
        -- Add row number for deduplication
        ROW_NUMBER() OVER (
            PARTITION BY
                contrib_name,
                committee_name,
                contrib_date,
                amount::decimal,
                address,
                city,
                state,
                zip_code,
                contrib_occupation
            ORDER BY 
                contrib_name,
                committee_name,
                contrib_date
        ) AS row_num
    FROM parsed_locations
    -- Filter for negative amount (returned transactions)
    WHERE amount::decimal >= 0
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
FROM cleaned_data
-- Keep only the first occurrence of each unique combination
-- It should be good to do this as now it has been partitioned on all
-- of the rows, whatever left must be duplicates
WHERE row_num = 1

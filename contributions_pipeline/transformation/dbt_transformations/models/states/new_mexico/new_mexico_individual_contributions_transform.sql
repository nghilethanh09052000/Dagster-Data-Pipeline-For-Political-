{{
    config(
        materialized='table',
        tags=["new_mexico", "contributions", "master"]
    )
}}

with transfomed as (

    SELECT
        'NM' AS source,

        'NM_' || {{ dbt_utils.generate_surrogate_key([
            'transaction_id',
            'transaction_date',
            'first_name',
            'last_name',
            'transaction_amount'
        ]) }} AS source_id,

        -- Generate committee_id using org_id
        'NM_' || org_id AS committee_id,

        -- Generate candidate_id based on candidate name fields for joining with candidates table
        -- Normalize to First, Last, Middle format to match candidates table
        CASE 
            WHEN candidate_first_name IS NOT NULL 
                 AND candidate_last_name IS NOT NULL 
                 AND TRIM(candidate_first_name) != '' 
                 AND TRIM(candidate_last_name) != ''
            THEN 'NM_' || org_id
            ELSE NULL
        END AS candidate_id,

        CASE 
            WHEN candidate_first_name IS NOT NULL 
                 AND candidate_last_name IS NOT NULL 
                 AND TRIM(candidate_first_name) != '' 
                 AND TRIM(candidate_last_name) != ''
            THEN TRIM(CONCAT_WS(' ',
                NULLIF(candidate_first_name, ''),
                NULLIF(candidate_middle_name, ''),
                NULLIF(candidate_last_name, '')
            ))
            ELSE NULL
        END AS candidate_name,

        -- Contributor name
        TRIM(CONCAT_WS(' ',
            NULLIF(first_name, ''),
            NULLIF(middle_name, ''),
            NULLIF(last_name, '')
        )) AS name,

        INITCAP(NULLIF(contributor_city, '')) AS city,
        UPPER(NULLIF(contributor_state, '')) AS state,
        NULLIF(contributor_zip_code, '') AS zip_code,

        NULLIF(contributor_employer, '') AS employer,
        NULLIF(contributor_occupation, '') AS occupation,

        CAST(NULLIF(transaction_amount, '') AS DECIMAL(10,2)) AS amount,

        -- Parse MM/DD/YYYY format
        to_timestamp(transaction_date, 'MM/DD/YYYY') AS contribution_datetime,

        CURRENT_TIMESTAMP AS insert_datetime

    FROM {{ source('nm', 'nm_contributions_landing') }}
    WHERE transaction_amount IS NOT NULL
      AND transaction_date IS NOT NULL
      AND (
          (first_name IS NOT NULL AND last_name IS NOT NULL)
          OR (candidate_first_name IS NOT NULL AND candidate_last_name IS NOT NULL)
      )

)

, final AS(
    SELECT
        source,
        source_id,
        committee_id,
        candidate_id,
        candidate_name,
        name,
        city,
        state,
        zip_code,
        employer,
        occupation,
        amount,
        contribution_datetime,
        insert_datetime
    FROM transfomed
)

SELECT * FROM final

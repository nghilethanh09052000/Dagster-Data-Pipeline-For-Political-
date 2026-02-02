{{ 
    config(
        materialized='table',
        tags=["wisconsin", "committee", "master"]
    ) 
}}

WITH source AS (
    SELECT 
        committee_id,
        committee_name,
        committee_address,
        registrant_status,
        election_date,
        branch,
        registered_date,
        amended_date,
        out_state,
        referendum,
        independent,
        exempt,
        termination_requested_date
    FROM {{ source('wi', 'wi_registered_committees_landing') }}
    WHERE registrant_status = 'Current'
),

final AS (
    SELECT
        'WI' AS source,
        'WI_' || committee_id AS committee_id,
        NULL AS committee_designation,  
        committee_name AS name,
        committee_address AS address1, 
        NULL AS address2,  
        NULL AS city,  
        'WI' AS state,  
        NULL AS zip_code,  
        NULL AS affiliation, 
        NULL AS district,  
        CURRENT_TIMESTAMP AS insert_datetime  
    FROM source
)

SELECT * FROM final

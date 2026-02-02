{{ 
    config(
        materialized='table',
        tags=["vermont", "candidate", "master"]
    ) 
}}

WITH raw as (
    SELECT
        'VT_' || filing_entity_id as candidate_id,
        CONCAT_WS(' ', candidate_first_name, candidate_middle_name, candidate_last_name) as name,
        NULL AS affiliation,
        NULL AS district
    FROM {{ source('vt', 'vermont_candidates_landing') }}
    WHERE 
        "filing_entity_id" <> '' 
        AND "filing_entity_id" <> 'Filing Entity Id'
)

, final as (
    SELECT
        'VT' as source,
        candidate_id,
        MAX(name) AS name,
        MAX(affiliation) AS affiliation,
        MAX(district) AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM raw
    GROUP BY candidate_id
)

SELECT * FROM final
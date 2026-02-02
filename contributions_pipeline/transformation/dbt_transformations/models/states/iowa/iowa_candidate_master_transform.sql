{{
    config(
        materialized = 'table',
        tags = ["iowa", "committee", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'IA' AS source,
        'IA_' || committee_number AS candidate_id,
        candidate_name as name,
        party AS affiliation,
        district AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('ia', 'ia_registered_political_candidates_landing') }}
    where candidate_name <> ''
)


SELECT * FROM source_data

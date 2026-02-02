{{
    config(
      materialized='table',
      tags=["illinois", "candidates", "master"]
    )
}}

WITH source_data AS (
    SELECT
        'IL' AS source,
        'IL_' || "ID" AS candidate_id,
        concat_ws(' ', "FirstName", "LastName") as name,
        "PartyAffiliation" AS affiliation,
        "District" AS district, 
        CURRENT_TIMESTAMP AS insert_datetime
    FROM {{ source('il', 'il_candidates_landing') }}

)

SELECT
    source,
    candidate_id,
    name,
    affiliation,
    district,
    insert_datetime
FROM source_data



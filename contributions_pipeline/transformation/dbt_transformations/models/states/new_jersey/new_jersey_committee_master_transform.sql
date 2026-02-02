{{
    config(
      materialized='table',
      tags=["new_jersey", "committee", "master"]
    )
}}

-- All of the independent (candidate) data are commented as I realised tha every year
-- the candidate basically becomes a PAC either way. So it's here just for documentation
-- purpose, if you need to use it feel free too!
WITH source_ind_data AS (
    SELECT
        rec_suffix,
        rec_fname,
        rec_mname,
        rec_lname,
        (
            CASE WHEN rec_suffix IS NOT NULL AND TRIM(rec_suffix) != '' THEN (' ' || rec_suffix) ELSE '' END
            || rec_fname
            || CASE WHEN rec_mname IS NOT NULL AND TRIM(rec_mname) != '' THEN (' ' || rec_mname) ELSE '' END
            || CASE WHEN rec_lname IS NOT NULL AND TRIM(rec_lname) != '' THEN (' ' || rec_lname) ELSE '' END
        ) AS "name",
        party,
        "location"
    FROM {{ source('nj', 'nj_contributions_landing') }}
    WHERE
        rec_fname IS NOT NULL
        AND TRIM(rec_fname) != ''
    GROUP BY
        rec_suffix,
        rec_fname,
        rec_mname,
        rec_lname,
        party,
        "location"
),

source_ind_data_transformed AS (
    SELECT
        'NJ' AS "source",
        'NJ_' || {{ dbt_utils.generate_surrogate_key(['"name"', 'party', 'location']) }} AS committee_id,
        "name",
        NULL AS committee_designation,
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        party AS affiliation,
        "location" AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_ind_data
    -- Re-deduped the data as we dedup the name
    GROUP BY
        "name",
        party,
        "location"
),

source_pac_data AS (
    SELECT
        rec_non_ind_name,
        rec_non_ind_name2,
        (
            rec_non_ind_name
            || CASE WHEN rec_non_ind_name2 IS NOT NULL AND TRIM(rec_non_ind_name2) != '' THEN (' ' || rec_non_ind_name2) ELSE '' END
        ) AS "name",
        party,
        "location"
    FROM {{ source('nj', 'nj_contributions_landing') }}
    WHERE
        rec_non_ind_name IS NOT NULL
        AND TRIM(rec_non_ind_name) != ''
    GROUP BY
        rec_non_ind_name,
        rec_non_ind_name2,
        party,
        "location"
),

source_pac_data_transformed AS (
    SELECT
        'NJ' AS "source",
        'NJ_' || {{ dbt_utils.generate_surrogate_key(['"name"', 'party', 'location']) }} AS committee_id,
        "name",
        NULL AS committee_designation,
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        party AS affiliation,
        "location" AS district,
        CURRENT_TIMESTAMP AS insert_datetime
    FROM source_pac_data
    -- Re-deduped the data as we dedup the name
    GROUP BY
        "name",
        party,
        "location"
)

SELECT
    source,
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM source_ind_data_transformed

UNION ALL

SELECT
    source,
    committee_id,
    committee_designation,
    "name",
    address1,
    address2,
    city,
    "state",
    zip_code,
    affiliation,
    district,
    insert_datetime
FROM source_pac_data_transformed

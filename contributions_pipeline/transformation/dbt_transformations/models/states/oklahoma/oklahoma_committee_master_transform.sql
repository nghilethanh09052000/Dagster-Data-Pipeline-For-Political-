{{ config(
    materialized='table',
    tags=["oklahoma", "committee", "master"]
) }}


WITH contrib_src AS (
    SELECT
        'OK_' || org_id AS committee_id,
        {{ generate_committee_id('OK_', 'committee_name') }} AS prototye_committee_id,
        null AS committee_designation,
        committee_name AS name,
        null AS address1,
        null AS address2,
        null as city,
        null as state,
        null AS zip_code,
        NULL AS affiliation,
        NULL AS district
    FROM {{ source('ok', 'ok_contributions_loans_landing') }}
    WHERE 
        org_id is not null and org_id <> ''
        and
        committee_name is not null and committee_name <> ''

)

, exp_src AS (
    SELECT
        'OK_' || org_id AS committee_id,
        {{ generate_committee_id('OK_', 'committee_name') }} AS prototye_committee_id,
        null AS committee_designation,
        committee_name AS name,
        null AS address1,
        null AS address2,
        null as city,
        null as state,
        null AS zip_code,
        NULL AS affiliation,
        NULL AS district
    FROM {{ source('ok', 'ok_expenditures_landing') }}
    WHERE 
        org_id is not null and org_id <> ''
        and
        committee_name is not null and committee_name <> ''
)

, contributions_and_expenditures_combined AS (
    SELECT DISTINCT * FROM contrib_src
    UNION ALL
    SELECT DISTINCT * FROM exp_src
)


, committee_src AS (
    SELECT
        {{ generate_committee_id('OK_', 'name') }} AS committee_id,
        name,
        CASE
            WHEN LOWER(type) = 'political action committee' THEN 'B'
            WHEN LOWER(type) IN ('non-oklahoma pac', 'special function committee', 'political party committee') THEN 'U'
            ELSE NULL
        END AS committee_designation
    FROM {{ source('ok', 'ok_committees_landing') }} 
    WHERE   
        name IS NOT NULL AND name <> ''
        AND status = 'Active'
)



, combined_committee_src as (
    SELECT
        b.committee_id,
        null as prototye_committee_id,
        a.committee_designation,
        a.name,
        null AS address1,
        null AS address2,
        null as city,
        null as state,
        null AS zip_code,
        NULL AS affiliation,
        NULL AS district
    FROM committee_src a
    LEFT JOIN contributions_and_expenditures_combined b
    ON a.committee_id = b.prototye_committee_id
    WHERE b.prototye_committee_id IS NOT NULL 

)

, union_data AS (
    SELECT DISTINCT * FROM combined_committee_src
    UNION ALL 
    SELECT DISTINCT * FROM contributions_and_expenditures_combined
)

, ranked_union_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY committee_id
            ORDER BY CASE WHEN committee_designation IS NOT NULL THEN 1 ELSE 2 END
        ) AS rn
    FROM union_data
)


SELECT
    'OK' as source,
    committee_id,
    committee_designation,
    name,
    address1,
    address2,
    city,
    state,
    zip_code,
    affiliation,
    district,
    CURRENT_TIMESTAMP AS insert_datetime
FROM ranked_union_data
WHERE rn = 1
ORDER BY committee_id
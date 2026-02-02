{{
    config(
      materialized='table',
      tags=["virginia", "committee", "master"]
    )
}}

-- Data docs: https://www.elections.virginia.gov/candidatepac-info/political-committees/

WITH old_pacs_data AS (
    -- Old PACs from schedule A
    SELECT
        'VA' AS "source",
        'VA_' || committee_code AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        -- Cannot be designation, not enough data to differentiate
        NULL AS committee_designation,
        committee_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY committee_code
        ) AS row_num
    FROM {{ source('va', 'schedulea_pac_old_landing') }}

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || committee_code AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        -- Cannot be designation, not enough data to differentiate
        NULL AS committee_designation,
        committee_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY committee_code
        ) AS row_num
    FROM {{ source('va', 'schedulea_pac_transitional_landing') }}

    UNION ALL

    --Old PACs from schedule B
    SELECT
        'VA' AS "source",
        'VA_' || committee_code AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        -- Cannot be designation, not enough data to differentiate
        NULL AS committee_designation,
        committee_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY committee_code
        ) AS row_num
    FROM {{ source('va', 'scheduleb_old_landing') }}

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || committee_code AS committee_id,
        'VA_' || {{ dbt_utils.generate_surrogate_key(['first_name', 'middle_name', 'last_name']) }} as candidate_id,
        trim(
            coalesce(nullif(first_name, '') || ' ', '')
            || coalesce(nullif(middle_name, '') || ' ', '')
            || last_name
        ) as candidate_name,
        'P' AS committee_designation,
        committee_name AS "name",
        NULL AS address1,
        NULL AS address2,
        NULL AS city,
        NULL AS "state",
        NULL AS zip_code,
        party AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY committee_code
        ) AS row_num
    FROM {{ source('va', 'scheduleb_old_landing') }}
),

numbered_old_pacs_data AS (
    -- Dedup PACs between eachother
    SELECT
        raw_data.source,
        raw_data.committee_id,
        raw_data.candidate_id,
        raw_data.candidate_name,
        raw_data.committee_designation,
        raw_data."name",
        raw_data.address1,
        raw_data.address2,
        raw_data.city,
        raw_data."state",
        raw_data.zip_code,
        raw_data.affiliation,
        raw_data.district,
        raw_data.insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data.committee_id
            -- Make sure we prioritze PACs with candidate if there's duplicate
            ORDER BY raw_data.candidate_id DESC NULLS LAST
        ) AS row_num
    FROM (
        -- Dedup PACs based on id on itself
        SELECT *
        FROM old_pacs_data
        WHERE row_num = 1
    ) AS raw_data
),

source_data AS (
    SELECT
        'VA' AS "source",
        'VA_' || "CommitteeCode" AS committee_id,
        'VA_' || "VoterRegistrationid" AS candidate_id,
        TRIM(
            COALESCE(NULLIF("CandidateFirstName", '') || ' ', '')
            || COALESCE(NULLIF("CandidateMiddleName", '') || ' ', '')
            || "CandidateLastName"
        ) AS candidate_name,
        'P' AS committee_designation,
        "CommitteeName" AS "name",
        "CommitteeStreetAddress" AS address1,
        "CommitteeSuite" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code,
        "PoliticalPartyName" AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "CommitteeCode"
        ) AS row_num
    FROM {{ source('va', 'va_candidate_campaign_committee_landing') }}
    WHERE
        "IsAmendment" != 'Yes'
        -- VA does some smoke test in prod, this is the candidate id they use
        AND "VoterRegistrationid" != '101025435'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || "CommitteeCode" AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        'U' AS committee_designation,
        "CommitteeName" AS "name",
        "CommitteeStreetAddress" AS address1,
        "CommitteeSuite" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "CommitteeCode"
        ) AS row_num
    FROM {{ source('va', 'va_inagural_committee_landing') }}
    WHERE
        "IsAmendment" != 'Yes'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || "CommitteeCode" AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        NULL AS committee_designation,
        "CommitteeName" AS "name",
        "CommitteeStreetAddress" AS address1,
        "CommitteeSuite" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code,
        "AffiliatedOrganizationName" AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "CommitteeCode"
        ) AS row_num
    FROM {{ source('va', 'va_political_action_committee_landing') }}
    WHERE
        "IsAmendment" != 'Yes'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || "CommitteeCode" AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        'U' AS committee_designation,
        "CommitteeName" AS "name",
        "CommitteeStreetAddress" AS address1,
        "CommitteeSuite" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code,
        "PartyAffiliation" AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "CommitteeCode"
        ) AS row_num
    FROM {{ source('va', 'va_political_party_committee_landing') }}
    WHERE
        "IsAmendment" != 'Yes'

    UNION ALL

    SELECT
        'VA' AS "source",
        'VA_' || "CommitteeCode" AS committee_id,
        NULL AS candidate_id,
        NULL AS candidate_name,
        'U' AS committee_designation,
        "CommitteeName" AS "name",
        "CommitteeStreetAddress" AS address1,
        "CommitteeSuite" AS address2,
        "CommitteeCity" AS city,
        "CommitteeState" AS "state",
        "CommitteeZipCode" AS zip_code,
        NULL AS affiliation,
        NULL AS district,
        CURRENT_TIMESTAMP AS insert_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY "CommitteeCode"
        ) AS row_num
    FROM {{ source('va', 'va_referendum_committee_landing') }}
    WHERE
        "IsAmendment" != 'Yes'

    UNION ALL

    SELECT *
    FROM numbered_old_pacs_data
)

SELECT
    source,
    committee_id,
    candidate_id,
    candidate_name,
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
FROM source_data
WHERE row_num = 1

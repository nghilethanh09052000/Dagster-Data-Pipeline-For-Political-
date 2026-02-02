{{
    config(
      materialized='table',
      tags=["texas", "candidates", "master"]
    )
}}

-- Tables docs: https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt
-- Codes (enums) docs: https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt

with source_data as (
    select
        filer_ident,
        filer_name_first,
        filer_name_last,
        row_number() over (
            partition by filer_ident
        ) as row_num
    from {{ source('tx', 'tx_filers_landing') }}
    where filer_type_cd = 'COH' or filer_type_cd = 'JCOH'
),

transformed_data as (
    select
        'TX' as "source",
        filer_ident,
        'TX_' || filer_ident as candidate_id,
        trim(coalesce(nullif(filer_name_first, '') || ' ', '') || filer_name_last) as "name",
        null as affiliation,
        current_timestamp as insert_datetime
    from source_data
    where row_num = 1
)

select
    "source",
    filer_ident,
    candidate_id,
    "name",
    affiliation,
    insert_datetime
from transformed_data

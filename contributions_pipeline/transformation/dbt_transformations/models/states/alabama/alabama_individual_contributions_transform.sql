{{ config(
    materialized='incremental',
    unique_key=['source_id'],
    tags=["alabama", "individual_contributions", "combined"]
) }}

{% set models = [
    {
        "name": "al_cash_contributions",
        "source_table": "al_cash_contributions",
        "id_column": "contribution_id",
        "amount_column": "contribution_amount",
        "date_column": "contribution_date",
    },
    {
        "name": "al_in_kind_contributions",
        "source_table": "al_in_kind_contributions",
        "id_column": "in_kind_contribution_id",
        "amount_column": "contribution_amount",
        "date_column": "contribution_date"
    },
    {
        "name": "al_other_receipts",
        "source_table": "al_other_receipts",
        "id_column": "receipt_id",
        "amount_column": "receipt_amount",
        "date_column": "receipt_date"
    }
] %}

{% for model in models %}
    {% if loop.first %}with{% else %},{% endif %} {{ model.name }} as (
        select
            'AL'::text as source,
            ('AL_' || {{ model.id_column }}) as source_id,
            ('AL_' || org_id) as committee_id,
            trim(coalesce(last_name, '') || ' ' || coalesce(first_name, '')) as name,
            city,
            upper(state) as state,
            zip as zip_code,
            null as employer,
            null as occupation,
            cast({{ model.amount_column }} as numeric(9,2)) as amount,
            cast({{ model.date_column }} as timestamp) as contribution_datetime,
            current_timestamp as insert_datetime
        from {{ source('al', model.source_table) }}
        where first_name <> '' and last_name <> ''
    )
{% endfor %}

, combined as (
    {% for model in models %}
        select * from {{ model.name }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select distinct * from combined

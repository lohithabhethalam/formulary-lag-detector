-- fct_formulary_lag.sql
--
-- Incremental mart built on stg_formulary_lag.
-- Only processes new application_numbers on each run,
-- making the model safe to re-run without duplicating data.

{{
    config(
        materialized='incremental',
        unique_key='application_number'
    )
}}

select
    application_number,
    drug_name,
    approval_date,
    marketing_start_date,
    lag_days,
    lag_weeks,
    lag_category,
    current_timestamp as loaded_at

from {{ ref('stg_formulary_lag') }}

{% if is_incremental() %}
where application_number not in (
    select application_number from {{ this }}
)
{% endif %}
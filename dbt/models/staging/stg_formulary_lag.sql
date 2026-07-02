-- stg_formulary_lag.sql
--
-- Deduplicates raw_formulary_lag, which contains multiple
-- parquet files from Delta Lake MERGE operations. Uses
-- row_number() to keep the most recent record per
-- application_number.

with deduped as (
    select
        application_number,
        drug_name,
        approval_date,
        marketing_start_date,
        lag_days,
        lag_weeks,
        lag_category,
        row_number() over (
            partition by application_number
            order by lag_days desc
        ) as rn
    from raw_formulary_lag
)

select
    application_number,
    drug_name,
    approval_date,
    marketing_start_date,
    lag_days,
    lag_weeks,
    lag_category

from deduped
where rn = 1
create or replace table `lotlinx-prod.lotlinx_prod_dataset.market_clean_dedup`
partition by snapshot_date
cluster by make, model, year, condition
as
with base as (
  select
    upper(trim(make)) as make,
    upper(trim(model)) as model,
    cast(year as int64) as year,
    upper(trim(condition)) as condition,
    safe_cast(avg_market_price_national as numeric) as avg_market_price_national,
    date(snapshot_date) as snapshot_date
  from `lotlinx-prod.lotlinx_prod_dataset.national_market_raw`
),

valid_keys as (
  select *
  from base
  where make is not null
    and make != ''
    and model is not null
    and model != ''
    and year is not null
    and condition is not null
    and condition != ''
    and snapshot_date is not null
),

dedup as (
  select *
  from valid_keys
  qualify row_number() over (
    partition by make, model, year, condition, snapshot_date
    order by avg_market_price_national desc nulls last
  ) = 1
)

select
  make,
  model,
  year,
  condition,
  avg_market_price_national,
  snapshot_date
from dedup;
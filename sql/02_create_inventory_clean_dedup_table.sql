create or replace table `lotlinx-prod.lotlinx_prod_dataset.inventory_clean_dedup`
partition by insert_timestamp
cluster by dealer_id, vin
as
with base as (
  select
    cast(dealer_id as int64) as dealer_id,
    upper(trim(vin)) as vin,
    upper(trim(make)) as make,
    upper(trim(model)) as model,
    cast(year as int64) as year,
    upper(trim(condition)) as condition,
    safe_cast(list_price as numeric) as list_price,
    date(insert_timestamp) as insert_timestamp
  from `lotlinx-prod.lotlinx_prod_dataset.inventory_raw`
),

valid_keys as (
  select *
  from base
  where dealer_id is not null
    and vin is not null
    and vin != ''
    and insert_timestamp is not null
),

dedup as (
  select *
  from valid_keys
  qualify row_number() over (
    partition by dealer_id, vin, insert_timestamp
    order by list_price desc nulls last
  ) = 1
)

select
  dealer_id,
  vin,
  make,
  model,
  year,
  condition,
  list_price,
  insert_timestamp
from dedup;
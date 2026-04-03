create or replace table `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`
partition by sold_timestamp
cluster by dealer_id, vin
as
with base as (
  select
    cast(dealer_id as int64) as dealer_id,
    upper(trim(vin)) as vin,
    safe_cast(sold_price as numeric) as sold_price,
    date(sold_timestamp) as sold_timestamp
  from `lotlinx-prod.lotlinx_prod_dataset.sales_raw`
),

valid_keys as (
  select *
  from base
  where dealer_id is not null
    and vin is not null
    and vin != ''
    and sold_timestamp is not null
),

dedup as (
  select *
  from valid_keys
  qualify row_number() over (
    partition by dealer_id, vin, sold_timestamp
    order by sold_price desc nulls last
  ) = 1
)

select
  dealer_id,
  vin,
  sold_price,
  sold_timestamp
from dedup;
create or replace table `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`
partition by sold_timestamp
cluster by dealer_id, vin
as
with max_sale_date as (
  select max(sold_timestamp) as max_sold_timestamp
  from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`
),

scoped_sales as (
  select s.*
  from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup` s
  cross join max_sale_date m
  where s.sold_timestamp between date_sub(m.max_sold_timestamp, interval 6 day)
                             and m.max_sold_timestamp
)

select
  dealer_id,
  vin,
  sold_price,
  sold_timestamp
from scoped_sales;
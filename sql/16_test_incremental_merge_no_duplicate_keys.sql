select
  dealer_id,
  vin,
  sold_timestamp,
  count(*) as row_count
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where sold_timestamp between (
  select min(sold_timestamp)
  from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`
) and (
  select max(sold_timestamp)
  from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`
)
group by 1, 2, 3
having count(*) > 1
order by row_count desc, sold_timestamp, dealer_id, vin;
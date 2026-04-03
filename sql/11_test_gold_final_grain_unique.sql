select
  dealer_id,
  vin,
  sold_timestamp,
  count(*) as row_count
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
group by 1, 2, 3
having count(*) > 1
order by row_count desc, sold_timestamp, dealer_id, vin;
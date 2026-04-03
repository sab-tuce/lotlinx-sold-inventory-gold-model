select
  g.dealer_id,
  g.vin,
  g.sold_timestamp,
  g.sold_price as gold_sold_price,
  s.sold_price as sales_clean_sold_price
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance` g
left join `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup` s
  on g.dealer_id = s.dealer_id
 and g.vin = s.vin
 and g.sold_timestamp = s.sold_timestamp
where g.sold_price is null
   or g.sold_price <= 0
order by g.sold_timestamp, g.dealer_id, g.vin;
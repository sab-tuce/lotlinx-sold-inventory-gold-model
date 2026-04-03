select
  count(*) as bad_sold_price_rows
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where sold_price is null
   or sold_price <= 0;
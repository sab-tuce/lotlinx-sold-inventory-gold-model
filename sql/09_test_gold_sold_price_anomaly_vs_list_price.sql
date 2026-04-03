select
  count(*) as anomalous_sold_price_rows
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where sold_price > 0
  and list_price > 0
  and (
    sold_price / list_price < 0.5
    or sold_price / list_price > 1.5
  );
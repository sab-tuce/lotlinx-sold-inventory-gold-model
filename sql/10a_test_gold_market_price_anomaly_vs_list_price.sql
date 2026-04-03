select
  count(*) as anomalous_market_price_rows
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where market_match_found = true
  and avg_market_price_national > 0
  and list_price > 0
  and (
    avg_market_price_national / list_price < 0.5
    or avg_market_price_national / list_price > 1.5
  );
select
  count(*) as bad_market_price_rows
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where market_match_found = true
  and (
    avg_market_price_national is null
    or avg_market_price_national <= 0
  );
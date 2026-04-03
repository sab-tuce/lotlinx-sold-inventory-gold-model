select
  count(*) as total_sales_rows,
  countif(market_match_found = true) as market_matched_rows,
  round(100 * countif(market_match_found = true) / count(*), 2) as market_join_rate_pct
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`;
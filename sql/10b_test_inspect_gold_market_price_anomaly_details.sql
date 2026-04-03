select
  dealer_id,
  vin,
  sold_timestamp,
  list_price,
  avg_market_price_national,
  round(avg_market_price_national / list_price, 3) as market_to_list_ratio,
  market_match_type,
  market_day_offset
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where market_match_found = true
  and avg_market_price_national > 0
  and list_price > 0
  and (
    avg_market_price_national / list_price < 0.5
    or avg_market_price_national / list_price > 1.5
  )
order by sold_timestamp, dealer_id, vin;
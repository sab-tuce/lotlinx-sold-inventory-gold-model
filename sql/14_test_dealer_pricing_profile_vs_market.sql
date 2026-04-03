select
  dealer_id,
  count(*) as comparable_rows,

  round(avg(sold_price), 2) as avg_sold_price,
  round(avg(list_price), 2) as avg_list_price,
  round(avg(avg_market_price_national), 2) as avg_market_price,

  round(avg(sold_price / list_price), 3) as avg_sold_to_list_ratio,
  round(avg(sold_price / avg_market_price_national), 3) as avg_sold_to_market_ratio,
  round(avg(avg_market_price_national / list_price), 3) as avg_market_to_list_ratio,

  countif(
    avg_market_price_national / list_price < 0.5
    or avg_market_price_national / list_price > 1.5
  ) as market_vs_list_anomaly_rows,

  round(
    100 * countif(
      avg_market_price_national / list_price < 0.5
      or avg_market_price_national / list_price > 1.5
    ) / count(*),
    2
  ) as market_vs_list_anomaly_rate_pct

from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where market_match_found = true
  and sold_price > 0
  and list_price > 0
  and avg_market_price_national > 0
group by dealer_id
order by market_vs_list_anomaly_rate_pct desc, comparable_rows desc;
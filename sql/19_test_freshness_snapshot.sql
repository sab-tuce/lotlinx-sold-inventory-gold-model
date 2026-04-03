select
  sales.max_sales_date,
  market.max_market_date,
  date_diff(current_date(), sales.max_sales_date, day) as sales_lag_days,
  date_diff(current_date(), market.max_market_date, day) as market_lag_days,
  case
    when date_diff(current_date(), sales.max_sales_date, day) <= 1 then 'OK'
    else 'LATE'
  end as sales_freshness_status,
  case
    when date_diff(current_date(), market.max_market_date, day) <= 2 then 'OK'
    else 'LATE'
  end as market_freshness_status
from (
  select max(sold_timestamp) as max_sales_date
  from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`
) sales
cross join (
  select max(snapshot_date) as max_market_date
  from `lotlinx-prod.lotlinx_prod_dataset.market_clean_dedup`
) market;
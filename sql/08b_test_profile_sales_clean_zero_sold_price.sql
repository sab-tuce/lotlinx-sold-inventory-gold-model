select
  sold_timestamp,
  dealer_id,
  count(*) as zero_sold_price_rows
from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`
where sold_price = 0
group by 1, 2
order by sold_timestamp, dealer_id;
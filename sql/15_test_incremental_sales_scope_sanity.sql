select
  count(*) as incremental_scope_rows,
  count(distinct format('%d|%s|%s', dealer_id, vin, cast(sold_timestamp as string))) as distinct_scope_keys,
  min(sold_timestamp) as min_scope_date,
  max(sold_timestamp) as max_scope_date
from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`;
with scope_keys as (
  select distinct
    dealer_id,
    vin,
    sold_timestamp
  from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`
)

select
  count(*) as missing_scope_keys_in_gold
from scope_keys s
left join `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance` g
  on s.dealer_id = g.dealer_id
 and s.vin = g.vin
 and s.sold_timestamp = g.sold_timestamp
where g.dealer_id is null;
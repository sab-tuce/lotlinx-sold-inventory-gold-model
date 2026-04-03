-- alter table `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
-- add primary key (dealer_id, vin, sold_timestamp) not enforced;

-- --------------------------------------------------------------

select
  table_name,
  constraint_name,
  constraint_type,
  enforced
from `lotlinx-prod.lotlinx_prod_dataset.INFORMATION_SCHEMA.TABLE_CONSTRAINTS`
where table_name = 'gold_sold_inventory_pricing_performance';
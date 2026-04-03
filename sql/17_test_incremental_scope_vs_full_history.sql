select
  (select count(*) from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`) as incremental_scope_rows,
  (select count(*) from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`) as full_sales_rows,
  round(
    100 * (select count(*) from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`)
    / (select count(*) from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`),
    2
  ) as incremental_scope_pct_of_full_history,
  (select min(sold_timestamp) from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`) as incremental_scope_start,
  (select max(sold_timestamp) from `lotlinx-prod.lotlinx_prod_dataset.incremental_sales_scope`) as incremental_scope_end,
  (select min(sold_timestamp) from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`) as full_history_start,
  (select max(sold_timestamp) from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup`) as full_history_end;
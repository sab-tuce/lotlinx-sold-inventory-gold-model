-- define the impacted market backfill window
declare backfill_start_date date default date('2026-02-26');
declare backfill_end_date   date default date('2026-02-28');

-- find the sold-date partitions in gold that were built
-- using market snapshots from the impacted date range
create temp table affected_sold_partitions as
select distinct sold_timestamp
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where market_snapshot_date between backfill_start_date and backfill_end_date;

-- rebuild the sales scope only for the affected sold-date partitions
create temp table backfill_sales_scope as
select s.*
from `lotlinx-prod.lotlinx_prod_dataset.sales_clean_dedup` s
join affected_sold_partitions p
  on s.sold_timestamp = p.sold_timestamp;

-- rebuild inventory as-of-sale only for the affected backfill sales scope
create temp table backfill_inventory_asof_sale as
with joined as (
  select
    s.dealer_id,
    s.vin,
    s.sold_timestamp,
    s.sold_price,

    i.make,
    i.model,
    i.year,
    i.condition,
    i.list_price,
    i.insert_timestamp as inventory_snapshot_date,

    row_number() over (
      partition by s.dealer_id, s.vin, s.sold_timestamp
      order by i.insert_timestamp desc, i.list_price desc nulls last
    ) as rn
  from backfill_sales_scope s
  left join `lotlinx-prod.lotlinx_prod_dataset.inventory_clean_dedup` i
    on s.dealer_id = i.dealer_id
   and s.vin = i.vin
   and i.insert_timestamp <= s.sold_timestamp
)

select
  dealer_id,
  vin,
  sold_timestamp,
  sold_price,
  make,
  model,
  year,
  condition,
  list_price,
  inventory_snapshot_date,
  case
    when inventory_snapshot_date is null then false
    else true
  end as inventory_match_found
from joined
where rn = 1;

-- rebuild market best-match only for the affected backfill scope
create temp table backfill_market_best_match as
with joined as (
  select
    ias.dealer_id,
    ias.vin,
    ias.sold_timestamp,
    ias.sold_price,
    ias.make,
    ias.model,
    ias.year,
    ias.condition,
    ias.list_price,
    ias.inventory_snapshot_date,
    ias.inventory_match_found,

    m.avg_market_price_national,
    m.snapshot_date as market_snapshot_date,

    case
      when m.snapshot_date = ias.sold_timestamp then 'exact'
      when m.snapshot_date < ias.sold_timestamp then 'prior'
      when m.snapshot_date > ias.sold_timestamp then 'future_within_3d'
      else 'no_match'
    end as market_match_type,

    case
      when m.snapshot_date is null then null
      else date_diff(m.snapshot_date, ias.sold_timestamp, day)
    end as market_day_offset,

    row_number() over (
      partition by ias.dealer_id, ias.vin, ias.sold_timestamp
      order by
        case
          when m.snapshot_date = ias.sold_timestamp then 0
          when m.snapshot_date < ias.sold_timestamp then 1
          when m.snapshot_date > ias.sold_timestamp then 2
          else 3
        end,
        case
          when m.snapshot_date <= ias.sold_timestamp
            then date_diff(ias.sold_timestamp, m.snapshot_date, day)
          else date_diff(m.snapshot_date, ias.sold_timestamp, day)
        end,
        m.snapshot_date desc
    ) as rn
  from backfill_inventory_asof_sale ias
  left join `lotlinx-prod.lotlinx_prod_dataset.market_clean_dedup` m
    on ias.make = m.make
   and ias.model = m.model
   and ias.year = m.year
   and ias.condition = m.condition
   and (
        m.snapshot_date <= ias.sold_timestamp
        or date_diff(m.snapshot_date, ias.sold_timestamp, day) between 1 and 3
   )
)

select
  dealer_id,
  vin,
  sold_timestamp,
  sold_price,
  make,
  model,
  year,
  condition,
  list_price,
  inventory_snapshot_date,
  inventory_match_found,
  avg_market_price_national,
  market_snapshot_date,
  market_match_type,
  market_day_offset,
  case
    when market_snapshot_date is null then false
    else true
  end as market_match_found
from joined
where rn = 1;

-- build the final recomputed rows for the affected backfill partitions
create temp table backfill_rows as
select
  dealer_id,
  vin,
  sold_timestamp,
  sold_price,
  make,
  model,
  year,
  condition,
  list_price,
  inventory_snapshot_date,
  inventory_match_found,
  avg_market_price_national,
  market_snapshot_date,
  market_match_type,
  market_day_offset,
  market_match_found,
  safe_divide(sold_price, avg_market_price_national) as price_to_market_pct,
  sold_price - avg_market_price_national as price_vs_market_amount,
  safe_divide(sold_price - avg_market_price_national, avg_market_price_national) as price_vs_market_pct,
  safe_divide(sold_price, list_price) as sold_to_list_pct,
  sold_price - list_price as sold_vs_listing_amount,
  list_price - avg_market_price_national as listing_vs_market_amount,
  safe_divide(list_price - avg_market_price_national, avg_market_price_national) as listing_vs_market_pct,
  case
    when avg_market_price_national is null then true
    else false
  end as market_price_missing_flag,
  case
    when list_price is null then true
    else false
  end as inventory_price_missing_flag
from backfill_market_best_match;

-- -----------------------------------------------------------------------------------part A test before transactions--------------------------
-- -- check how many sold-date partitions were affected by the backfill
-- select count(*) as affected_partition_count
-- from affected_sold_partitions;

-- -- check how many recomputed rows were prepared for reinsertion
-- select count(*) as recomputed_row_count
-- from backfill_rows;


-- -----------------------------------------------------------------------------------part B running transactions--------------------------
-- replace only the affected sold-date partitions in the gold table
begin transaction;

delete from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
where sold_timestamp in (
  select sold_timestamp
  from affected_sold_partitions
);

insert into `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance` (
  dealer_id,
  vin,
  sold_timestamp,
  sold_price,
  make,
  model,
  year,
  condition,
  list_price,
  inventory_snapshot_date,
  inventory_match_found,
  avg_market_price_national,
  market_snapshot_date,
  market_match_type,
  market_day_offset,
  market_match_found,
  price_to_market_pct,
  price_vs_market_amount,
  price_vs_market_pct,
  sold_to_list_pct,
  sold_vs_listing_amount,
  listing_vs_market_amount,
  listing_vs_market_pct,
  market_price_missing_flag,
  inventory_price_missing_flag
)
select
  dealer_id,
  vin,
  sold_timestamp,
  sold_price,
  make,
  model,
  year,
  condition,
  list_price,
  inventory_snapshot_date,
  inventory_match_found,
  avg_market_price_national,
  market_snapshot_date,
  market_match_type,
  market_day_offset,
  market_match_found,
  price_to_market_pct,
  price_vs_market_amount,
  price_vs_market_pct,
  sold_to_list_pct,
  sold_vs_listing_amount,
  listing_vs_market_amount,
  listing_vs_market_pct,
  market_price_missing_flag,
  inventory_price_missing_flag
from backfill_rows;

commit transaction;


-- -----------------------------------------------------------------------------------part B test of running transactions--------------------------
-- validate final gold row count and market coverage after backfill
select
  count(*) as total_rows,
  countif(market_match_found) as market_matched_rows,
  countif(price_to_market_pct is not null) as price_to_market_populated_rows
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`;

-- validate final grain uniqueness after backfill
select
  dealer_id,
  vin,
  sold_timestamp,
  count(*) as cnt
from `lotlinx-prod.lotlinx_prod_dataset.gold_sold_inventory_pricing_performance`
group by 1,2,3
having count(*) > 1;
# Lotlinx Sold Inventory Pricing Performance Gold Model

This repository contains my solution for the Lotlinx take-home assignment.

The goal of this project is to design a production-ready gold-layer data model in BigQuery to support a Sold Inventory Pricing Performance Dashboard. The model allows a dealership manager to evaluate whether each sold vehicle was priced above or below the national market average at the time of sale, and how the original listing price compared to that benchmark.

## Repository Structure

- `docs/`: written assignment deliverables
- `sql/`: BigQuery SQL used to build the cleaned, tested, and production-style gold model
- `screenshots/`: supporting screenshots from the BigQuery environment

## Final Model Grain

One row per sold vehicle event, identified by dealer_id + vin + sold_timestamp.

## Key Design Choices

- Sales is treated as the core event table.
- Inventory is joined using the latest valid listing record at or before the sale timestamp.
- National market data is matched using closest-date logic, prioritizing exact date and then nearest prior date.
- Raw duplicates are removed before gold model construction.
- The final dataset includes quality checks, join-rate monitoring, incremental merge logic, and freshness validation.

## How to Review

1. Read the written submission in `docs/`
2. Review the SQL build logic in `sql/`
3. Review validation and monitoring SQL files for production-readiness

# Execution Evidence — Unity Catalog (governance)

Component: **Unity Catalog — govern it.**

## Delta catalog/schema
`serverless_stable_mv5gxq_catalog.insurance_trip_wise` — tables:
```
agg_driver_conversions   agg_location            agg_offer_performance
brickwise_device_data    brickwise_device_data_history
dim_driver_sync          dim_drivers             dim_products
fact_conversions         fact_offers             ml_premium_features
policies                 products                trips
(+ event_log_* for the pipeline)
```

## Lakebase surfaced as a governed online UC catalog
`av_lakebase_tripwise` — `catalog_type = MANAGED_ONLINE_CATALOG`
(connection `managed-database-catalog-…`). Schema `tripwise_demo`, table `devices`
queryable through the SQL warehouse (used by the app's Driver Details panel).

## Access control (grants applied this session)
App SP `faee39eb-c8e5-4d99-ada7-4deca1cea3e4`:
- **Unity Catalog** on `av_lakebase_tripwise`: `USE_CATALOG`, `USE_SCHEMA` (tripwise_demo),
  `SELECT` on `devices`.
- **Lakebase Postgres**: `USAGE` on `insurance_trip_wise`, `SELECT` on
  `dim_driver_sync` (+ prior `SELECT` on `tripwise_demo.devices`).

See [lakebase-uc-grants.md](../docs/lakebase-uc-grants.md) for the exact commands.

## How to reproduce
```bash
databricks api post /api/2.0/sql/statements -p fevm-target --json '{
 "warehouse_id":"22ad910fe1aed92d",
 "statement":"SELECT table_name FROM serverless_stable_mv5gxq_catalog.information_schema.tables WHERE table_schema='\''insurance_trip_wise'\'' ORDER BY 1"}'
databricks catalogs get av_lakebase_tripwise -p fevm-target -o json
databricks grants get table av_lakebase_tripwise.tripwise_demo.devices -p fevm-target -o json
```

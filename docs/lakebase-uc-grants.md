# Grants to the app service principal

App SP: `faee39eb-c8e5-4d99-ada7-4deca1cea3e4`

The app connects to data two ways, so grants were needed in **two** permission systems.

## 1. Lakebase Postgres — `dim_driver_sync`
Needed for the agent list + offer recommendation (psycopg2 path).
Connect as the instance owner and grant:
```sql
GRANT USAGE  ON SCHEMA insurance_trip_wise                 TO "faee39eb-c8e5-4d99-ada7-4deca1cea3e4";
GRANT SELECT ON insurance_trip_wise.dim_driver_sync        TO "faee39eb-c8e5-4d99-ada7-4deca1cea3e4";
```
Admin connection:
```bash
HOST=$(databricks postgres list-endpoints projects/tripwise/branches/production -p fevm-target -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential projects/tripwise/branches/production/endpoints/primary -p fevm-target -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p fevm-target -o json | jq -r '.userName')
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require"
```
(The app SP already had SELECT on `tripwise_demo.devices` from prior setup.)

## 2. Unity Catalog — online catalog `av_lakebase_tripwise`
Needed for the Driver Details query, which reads
`av_lakebase_tripwise.tripwise_demo.devices` through the SQL warehouse.
```bash
SP=faee39eb-c8e5-4d99-ada7-4deca1cea3e4
databricks grants update catalog av_lakebase_tripwise \
  --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"USE_CATALOG\"]}]}" -p fevm-target
databricks grants update schema av_lakebase_tripwise.tripwise_demo \
  --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"USE_SCHEMA\"]}]}" -p fevm-target
databricks grants update table av_lakebase_tripwise.tripwise_demo.devices \
  --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"SELECT\"]}]}" -p fevm-target
```

## Lakebase tables available (for reference)
| Schema.table | Notes |
|--------------|-------|
| `tripwise_demo.devices` | registration table (1000 rows) |
| `insurance_trip_wise.dim_driver_sync` | driver dim synced from Delta (1500 rows) |
| `insurance_trip_wise.agg_location_sync` | location aggregates |

`policies` and the trip-behavior tables are **not** in Lakebase — they remain Delta-only,
which is why the detail panel / forecast / policy-update still use the SQL warehouse.

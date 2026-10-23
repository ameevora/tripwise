# Execution Evidence — Lakebase (operational serving)

Component: **Lakebase — operational serving.** Autoscaling project
`projects/tripwise/branches/production/endpoints/primary`, db `databricks_postgres`.
The app reads these Lakebase tables live via psycopg2 (OAuth token).

## Table row counts
```
         t         | count
-------------------+-------
 devices           |  1000
 dim_driver_sync   |  1500
 agg_location_sync |    54
```

## `tripwise_demo.devices` — registration table (serves offer tab + agent list names)
```
 device_id |   driver_name   |       registered_at        | registration_latitude | registration_longitude
-----------+-----------------+----------------------------+-----------------------+------------------------
 DEV1000   | Isabella Kim    | 2026-10-22 21:54:12.986199 |  41.89534343008152    | -87.62983122830433
 DEV0999   | Wyatt Fernandez | 2026-10-22 21:54:12.982700 |  41.816142206069294   | -87.68767732692794
 DEV0998   | Luca Takahashi  | 2026-10-22 21:54:12.978353 |  37.78137788791131    | -122.40064435778355
 DEV0997   | Theodore Olsen  | 2026-10-22 21:54:12.974356 |  40.71975896380654    | -74.01758673762433
 DEV0996   | Carter Rossi    | 2026-10-22 21:54:12.970454 |  25.813588427514507   | -80.17831950461888
```

## `insurance_trip_wise.dim_driver_sync` — driver dim (serves scores/risk)
```
 device_id |  driver_name  | current_driving_score | risk_tier | total_trips
-----------+---------------+-----------------------+-----------+-------------
 DEV0178   | Beverly Hicks |                 100   | low       |           2
 DEV0154   | Janice Berry  |                  98.2 | low       |           3
 DEV1161   | Liam Burke    |                  98   | low       |          10
 DEV0260   | Amy Stewart   |                  96.3 | low       |           2
 DEV1107   | Roger Fisher  |                  96.2 | low       |           5
```

## How to reproduce
```bash
HOST=$(databricks postgres list-endpoints projects/tripwise/branches/production -p fevm-target -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential projects/tripwise/branches/production/endpoints/primary -p fevm-target -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p fevm-target -o json | jq -r '.userName')
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" \
  -c "SELECT count(*) FROM tripwise_demo.devices;" \
  -c "SELECT count(*) FROM insurance_trip_wise.dim_driver_sync;" \
  -c "SELECT count(*) FROM insurance_trip_wise.agg_location_sync;"
```

> All synthetic data — no customer records.

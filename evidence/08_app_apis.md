# Execution Evidence — Databricks App (API responses)

Component: **A Databricks App — surface it to the business.**
App `tripwise` (https://<app-url>). Below are the
results the app's endpoints return, produced by running their underlying queries as the
app service principal.

## `/api/drivers` — Agent tab Select Driver list
Query: `tripwise_demo.devices` JOIN `insurance_trip_wise.dim_driver_sync` (name from
`devices`, score/risk from `dim_driver_sync`), `status='active'`, ordered by score.
```
device_id   driver_name        current_driving_score   risk_tier
DEV0178     Caleb Dubois       100                     low
DEV0154     Harper Takahashi   98.2                    low
DEV0260     Nova Yamamoto      96.3                    low
DEV0192     Sophia Hoffman     95.3                    low
DEV0455     Luca Tanaka        94.5                    low
```

## `/api/offer_recommendation/<device_id>` — Offer tab recommendation (DEV0001)
Query: `tripwise_demo.devices` LEFT JOIN `insurance_trip_wise.dim_driver_sync`, then a
state-based trigger lookup in `agg_location_sync`.
```
 device_id |   driver_name    | registration_latitude | registration_longitude | current_driving_score
-----------+------------------+-----------------------+------------------------+-----------------------
 DEV0001   | Theodore Shimizu |  29.79941669728652    | -95.40155570005975     |  78.4
```
The driving score (`78.4`) is surfaced in the offer recommendation panel's dedicated
Driving Score tile.

## How to reproduce
```bash
HOST=$(databricks postgres list-endpoints projects/tripwise/branches/production -p fevm-target -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential projects/tripwise/branches/production/endpoints/primary -p fevm-target -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p fevm-target -o json | jq -r '.userName')
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" -c "
SELECT dev.device_id, dev.driver_name, s.current_driving_score, s.risk_tier
FROM tripwise_demo.devices dev
JOIN insurance_trip_wise.dim_driver_sync s ON dev.device_id=s.device_id
WHERE s.status='active' ORDER BY s.current_driving_score DESC LIMIT 5;"
```

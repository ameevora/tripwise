# fevm-target — TripWise Demo Objects

Local snapshot of the TripWise demo objects deployed to the **`fevm-target`** workspace
(`https://fevm-serverless-stable-mv5gxq.cloud.databricks.com`, workspace id
`7474658320355593`). Migrated from `demo_west`; catalog `financial_services` →
`serverless_stable_mv5gxq_catalog`.

Synced from the workspace with:
```bash
databricks workspace export-dir /Users/amee.vora@databricks.com/tripwise   fevm-target/tripwise   -p fevm-target --overwrite
databricks workspace export-dir /Users/amee.vora@databricks.com/notebooks  fevm-target/notebooks  -p fevm-target --overwrite
```

## Contents
| Path | What |
|------|------|
| `tripwise/` | Databricks App source (Flask) — `app.py`, `app.yaml`, `config/`, `templates/`, `static/` |
| `notebooks/` | 13 data / ML / Zerobus notebooks |
| `dashboards/tripwise_offer_performance.lvdash.json` | AI/BI dashboard definition |
| `genie/insurance_trip_and_driver_analytics.json` | Genie space definition |

## Deployed object IDs (fevm-target)
| Object | ID / name |
|--------|-----------|
| App | `tripwise` — https://tripwise-7474658320355593.aws.databricksapps.com |
| App service principal | `faee39eb-c8e5-4d99-ada7-4deca1cea3e4` |
| Lakebase project (autoscaling) | `projects/tripwise`, endpoint `ep-soft-frost-d263qfy8…`, db `databricks_postgres` |
| UC catalog/schema (Delta) | `serverless_stable_mv5gxq_catalog.insurance_trip_wise` (13 tables) |
| Lakebase schemas | `tripwise_demo.devices` (2000), `insurance_trip_wise.agg_location_sync` (54) |
| Warehouse | `22ad910fe1aed92d` |
| Dashboard | `01f15bdebe6a12d8b153f14e54703b8e` |
| Genie space | `01f15d6cfd4a14c29378cffba250e64b` |
| Job — Device Telemetry Data | `420962304813966` |
| Job — Register Devices - Lakebase Streaming | `213721155994897` |
| Job — Premium Forecast: Train & Deploy | `93417101944426` (create_feature_table → train_premium_model → deploy_endpoint) |
| ML model | `serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model` v1 (`@champion`) |
| Serving endpoint | `premium_forecast_tripwise` (id `e99e504b608d497faa2a3f4f880913b2`) — serves discount prediction for the app's agent_action tab |
| MLflow artifacts volume | `serverless_stable_mv5gxq_catalog.insurance_trip_wise.vl_premium_forecast_tripwise` |

## Related docs
- `../docs/LAKEBASE_APP_SP_AUTH_FIX.md` — Lakebase service-principal auth fix (error → cause → fix)

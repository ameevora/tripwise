# Execution Evidence — TripWise (fevm-target)

Text-readable proof that each component of the solution **actually ran** (per the Build
domain criteria: committed outputs, run records, query results, real model output — not
screenshots). All data is synthetic; no customer data.

| # | Component | Evidence file | Proof type |
|---|-----------|---------------|-----------|
| 01 | Lakeflow Connect / Zerobus ingest | [01_ingest_zerobus.md](01_ingest_zerobus.md) + [notebooks/ingest_telemetry_zerobus.ipynb](notebooks/ingest_telemetry_zerobus.ipynb) | notebook output ("ingested 5000 records") + table count (11,101 rows / 1000 devices) |
| 02 | Lakebase device registration | [02_registration_lakebase.md](02_registration_lakebase.md) | job run `<run-id>` SUCCESS + table state correlated to run window |
| 03 | Unity Catalog governance | [03_unity_catalog.md](03_unity_catalog.md) | table inventory + online catalog + grants |
| 04 | Lakebase operational serving | [04_lakebase_serving.md](04_lakebase_serving.md) | row counts + samples of `devices`, `dim_driver_sync`, `agg_location_sync` |
| 05 | ML: premium/discount forecast | [05_ml_premium_forecast.md](05_ml_premium_forecast.md) + [notebooks/](notebooks/) | run `xgboost_discount_v1` metrics + endpoint READY + **live prediction (5.52)** |
| 06 | Gen AI: personalized email | [06_genai_email.md](06_genai_email.md) | real llama-3.3-70b output |
| 07 | Genie: NL querying | [07_genie.md](07_genie.md) | NL question → generated SQL → grounded answer (Extended Zone Protection, 133) |
| 08 | Databricks App | [08_app_apis.md](08_app_apis.md) | `/api/drivers` + `/api/offer_recommendation` responses |

## Notebooks (committed with cell outputs)
- `notebooks/ingest_telemetry_zerobus.ipynb` — 5 cells with output
- `notebooks/create_feature_table.ipynb` — 3 cells with output
- `notebooks/train_premium_model.ipynb` — 8 cells with output

> `register_devices_streaming` and `deploy_endpoint` don't persist stdout to the notebook
> (streaming / job execution); their evidence is captured as run records + resulting
> state in `02_*` and `05_*`.

## Environment
- Workspace: `fevm-target` (`https://<workspace-url>`)
- Delta catalog: `serverless_stable_mv5gxq_catalog.insurance_trip_wise`
- Lakebase: `projects/tripwise` (+ online UC catalog `av_lakebase_tripwise`)
- App SP: `faee39eb-c8e5-4d99-ada7-4deca1cea3e4`

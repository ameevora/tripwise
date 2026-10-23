# Reference-architecture coverage

Does the TripWise demo include each required building block? **Yes — all six.**

| Component | Included | How (evidence) |
|-----------|----------|----------------|
| **Lakeflow Connect (Zerobus)** — ingest raw data (synthetic/public) | ✅ | Synthetic telematics generated and ingested via **Zerobus (Lakeflow Connect)** — the direct-write streaming ingestion API. **Device Telemetry Data** job (`420962304813966`) runs `ingest_telemetry_zerobus` → Zerobus ingest into Delta `…insurance_trip_wise.brickwise_device_data`. **Register Devices** job (`213721155994897`) streams device registrations into Lakebase. `load_sample_data*` notebooks generate synthetic base data. Orchestrated as Lakeflow Jobs. |
| **Unity Catalog** — govern it | ✅ | All analytical tables in `serverless_stable_mv5gxq_catalog.insurance_trip_wise` (UC). Lakebase surfaced as a **governed online UC catalog** `av_lakebase_tripwise`. Access controlled via UC grants (`USE_CATALOG`/`USE_SCHEMA`/`SELECT`) + Lakebase federated roles. |
| **Lakebase** — operational serving | ✅ | Autoscaling Lakebase project `projects/tripwise`. Serves the app's operational reads: device registrations (`tripwise_demo.devices`), driver dim (`insurance_trip_wise.dim_driver_sync`), location aggregates (`agg_location_sync`). Agent list + offer tab read Lakebase live via psycopg2. |
| **ML or Gen AI** — make it intelligent | ✅ (both) | **ML:** `premium_forecast_tripwise` serving endpoint (READY) serving `premium_forecast_model`; **TripWise Premium Forecast** job (`93417101944426`) does feature → train → deploy; used for discount/premium prediction (`app.py` ~L851). **Gen AI:** personalized email generation via Foundation Model endpoint `databricks-meta-llama-3-3-70b-instruct` (`w.serving_endpoints.query`, `app.py` L705/971/1154). |
| **Genie Agent** — natural-language queryable | ✅ | Genie space **Insurance Trip and Driver Analytics** (`01f15d6cfd4a14c29378cffba250e64b`), embedded in the app's Genie tab (`genie_embedding.py`, `/api/genie/start_conversation`, `/api/genie/continue_conversation`). |
| **Databricks App** — surface to the business | ✅ | Flask app **`tripwise`** deployed as a Databricks App (https://tripwise-7474658320355593.aws.databricksapps.com). Tabs: Analytics, Insights, Genie, Agent Actions, Offer Actions, Registrations. |

## End-to-end flow
Synthetic telematics → **Zerobus (Lakeflow Connect) ingest** → **Delta (Unity Catalog)** →
**ML training + serving** (premium/discount) & **Gen AI** (email) → **Lakebase** for
operational serving → **Databricks App** (with embedded **Genie** NL querying) to the
business user.

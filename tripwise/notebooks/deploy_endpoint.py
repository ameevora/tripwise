# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Serving Endpoint: `premium_forecast_tripwise`
# MAGIC
# MAGIC Creates (or updates) a Databricks Model Serving endpoint that wraps
# MAGIC `serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model@champion`.
# MAGIC
# MAGIC The endpoint accepts a driver's feature vector and returns:
# MAGIC - `predicted_discount_pct` — recommended discount to apply (%)
# MAGIC
# MAGIC The adjusted premium is derived by the caller as:
# MAGIC `adjusted_premium = base_premium * (1 - predicted_discount/100)`
# MAGIC where `base_premium` is the undiscounted `premium_amount` stored in the policy.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. `create_feature_table` notebook has been run
# MAGIC 2. `train_premium_model` notebook has been run and model is registered with `@champion` alias

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput,
)
import time

w = WorkspaceClient()

ENDPOINT_NAME = "premium_forecast_tripwise"
MODEL_NAME    = "serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model"
MODEL_VERSION = "1"   # override below if you want the latest version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve champion version number

# COMMAND ----------

from mlflow.tracking import MlflowClient
import mlflow

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

versions = client.search_model_versions(f"name='{MODEL_NAME}'")
champion_version = None
for v in versions:
    aliases = client.get_model_version(MODEL_NAME, v.version).aliases
    if "champion" in aliases:
        champion_version = v.version
        break

if champion_version is None:
    raise ValueError(
        f"No version with alias 'champion' found for model '{MODEL_NAME}'. "
        "Run train_premium_model notebook first."
    )

print(f"Deploying model version: {champion_version}  (alias: champion)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create or update the serving endpoint

# COMMAND ----------

served_entity = ServedEntityInput(
    entity_name=MODEL_NAME,
    entity_version=champion_version,
    scale_to_zero_enabled=True,        # cost-efficient for demo workloads
    workload_size="Small",             # 1 concurrent request; bump to Medium for production
)

endpoint_config = EndpointCoreConfigInput(
    served_entities=[served_entity],
)

# Check if endpoint already exists
existing_endpoints = [e.name for e in w.serving_endpoints.list()]

if ENDPOINT_NAME in existing_endpoints:
    print(f"Endpoint '{ENDPOINT_NAME}' exists — updating served entity to version {champion_version}...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=[served_entity],
    )
    print("Update submitted.")
else:
    print(f"Creating new endpoint '{ENDPOINT_NAME}'...")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=endpoint_config,
    )
    print("Creation submitted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Wait until the endpoint is ready

# COMMAND ----------

print("Waiting for endpoint to reach READY state (this can take 5-10 min for a new endpoint)...")

MAX_WAIT_MINUTES = 15
poll_interval    = 30   # seconds

for attempt in range(MAX_WAIT_MINUTES * 60 // poll_interval):
    endpoint = w.serving_endpoints.get(name=ENDPOINT_NAME)
    state    = endpoint.state.ready.value if endpoint.state and endpoint.state.ready else "UNKNOWN"
    config_update = endpoint.state.config_update.value if endpoint.state and endpoint.state.config_update else ""

    print(f"  [{attempt * poll_interval:>4}s] ready={state}  config_update={config_update}")

    if state == "READY":
        print(f"\nEndpoint '{ENDPOINT_NAME}' is READY.")
        break
    time.sleep(poll_interval)
else:
    print(f"\nTimeout after {MAX_WAIT_MINUTES} minutes. Check the Serving UI for details.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Smoke-test the live endpoint

# COMMAND ----------

# Build a minimal sample payload using the feature schema from training.
# CURRENT_PREMIUM is the already-discounted premium_amount from the last policy.
CURRENT_PREMIUM  = 142.50
CURRENT_DISCOUNT = 10.0     # existing discount_pct on that policy (%)

sample_payload = {
    "dataframe_records": [
        {
            "current_driving_score": 82.5,
            "total_trips": 45,
            "total_miles": 1230.0,
            "avg_trip_score": 79.3,
            "avg_trip_distance_miles": 27.3,
            "avg_speed_mph": 38.1,
            "avg_rapid_accel_count": 0.3,
            "avg_sharp_turn_count": 0.5,
            "avg_leaving_home_zone_count": 0.4,
            "avg_rough_road_conditions_count": 0.2,
            "avg_night_driving_pct": 0.12,
            "avg_high_risk_area_pct": 0.08,
            "avg_inclement_weather_pct": 0.05,
            "embedded_conversion_rate": 0.22,
            "risk_tier": "low",
            "state": "CA",
            "coverage_level": "standard",
            "policy_type": "annual",
        }
    ]
}

response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_records=sample_payload["dataframe_records"],
)

predictions = response.predictions
# Model returns a single value: predicted discount_pct
predicted_discount = float(predictions[0])
undiscounted_base  = CURRENT_PREMIUM / (1 - CURRENT_DISCOUNT / 100)
adjusted_premium   = undiscounted_base * (1 - predicted_discount / 100)

print("Smoke-test predictions:", predictions)
print(f"\n  Current premium (discounted) : ${CURRENT_PREMIUM:.2f}")
print(f"  Current discount             : {CURRENT_DISCOUNT:.1f}%")
print(f"  Undiscounted base            : ${undiscounted_base:.2f}")
print(f"  Predicted discount           : {predicted_discount:.1f}%")
print(f"  Adjusted premium             : ${adjusted_premium:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary
# MAGIC
# MAGIC | Item | Value |
# MAGIC |------|-------|
# MAGIC | Endpoint name | `premium_forecast_tripwise` |
# MAGIC | Model | `serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model` |
# MAGIC | Version deployed | champion |
# MAGIC | Scale-to-zero | enabled |
# MAGIC | Workload size | Small |
# MAGIC
# MAGIC The Flask app reads the endpoint name from the env var:
# MAGIC ```
# MAGIC PREMIUM_FORECAST_ENDPOINT=premium_forecast_tripwise
# MAGIC ```
# MAGIC
# MAGIC To redeploy after retraining, just re-run `train_premium_model` (it updates the `@champion` alias)
# MAGIC then re-run this notebook — it will issue an `update_config` call to roll the endpoint to the new version.
# Execution Evidence — ML: Premium/Discount Forecast (train → deploy → serve)

Component: **ML (make it intelligent)**.
Job: *TripWise Premium Forecast - Train & Deploy* (`93417101944426`) →
`create_feature_table` → `train_premium_model` → `deploy_endpoint`.
Model: `serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model`.
Endpoint: `premium_forecast_tripwise`.

## 1. Training run — `xgboost_discount_v1`
MLflow experiment `1519050935243382`. Two finished runs (one per model version);
the **served** one is tied to model **version 1**:

| Run | run_id | Model ver | mae_discount_pct | r2_discount_pct | Status |
|-----|--------|-----------|------------------|-----------------|--------|
| xgboost_discount_v1 | `947b4ff1af4948b6ae4cc51f41db86e2` | **v1 (served)** | **1.5423** | -0.1258 | FINISHED |
| xgboost_discount_v1 | `863d8ca0f8454b65934edf56d74aef5b` | v2 | 1.5337 | -0.121 | FINISHED |

Params (served run v1):
```
tree_method=hist  n_estimators=300  max_depth=6  learning_rate=0.05
subsample=0.8  colsample_bytree=0.8  random_state=42  test_size=0.2  train_rows=1200
```
> `r2` is slightly negative on this synthetic dataset — committed as-is (honest model output).

## 2. Registered model versions (Unity Catalog)
```
version 1  run 947b4ff1af4948b6ae4cc51f41db86e2  status READY
version 2  run 863d8ca0f8454b65934edf56d74aef5b  status READY
```

## 3. Deployed serving endpoint — `premium_forecast_tripwise`
```
served_entity: serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model
entity_version: 1
state: DEPLOYMENT_READY (scale-to-zero)
```

## 4. Live prediction (real model output)
Input — one row from `ml_premium_features`:
```json
{"current_driving_score":"78.4","total_trips":"3","total_miles":"50.3",
 "avg_trip_score":"76.7","avg_trip_distance_miles":"11.07","avg_speed_mph":"22.93",
 "avg_rapid_accel_count":"0.333","avg_sharp_turn_count":"0.333",
 "avg_leaving_home_zone_count":"0.667","avg_rough_road_conditions_count":"1.333",
 "avg_night_driving_pct":"17.9","avg_high_risk_area_pct":"14.9333",
 "avg_inclement_weather_pct":"24.4","embedded_conversion_rate":"0.3333",
 "risk_tier":"medium","state":"PA","coverage_level":"standard","policy_type":"full_term"}
```
Output — endpoint response:
```json
{ "predictions": [ 5.521693229675293 ] }
```
Interpretation: predicted `discount_pct ≈ 5.52%` for this driver (the app clamps to
[0,50] and derives the adjusted premium from it).

## How to reproduce
```bash
# training run metrics
databricks api post /api/2.0/mlflow/runs/search \
  --json '{"experiment_ids":["1519050935243382"],"max_results":20}' -p fevm-target

# endpoint served version + state
databricks serving-endpoints get premium_forecast_tripwise -p fevm-target -o json

# live prediction (feature record pulled from ml_premium_features)
databricks serving-endpoints query premium_forecast_tripwise \
  --json '{"dataframe_records":[ <feature record> ]}' -p fevm-target
```

> All inputs are synthetic feature rows — no customer data.

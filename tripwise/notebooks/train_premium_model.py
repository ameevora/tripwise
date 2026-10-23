# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Premium Forecast — Model Training & UC Registration
# MAGIC
# MAGIC Trains an XGBoost regressor to predict:
# MAGIC - `discount_pct` — recommended discount percentage to apply
# MAGIC
# MAGIC The adjusted premium is derived at inference time by the serving app using the
# MAGIC live `premium_amount` and `discount_pct` from the `policies` table.
# MAGIC
# MAGIC ## Inputs
# MAGIC `serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features`
# MAGIC (run `create_feature_table` notebook first)
# MAGIC
# MAGIC ## Output
# MAGIC Registered model: `serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model`

# COMMAND ----------

# MAGIC %pip install xgboost scikit-learn mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

CATALOG  = "serverless_stable_mv5gxq_catalog"
SCHEMA   = "insurance_trip_wise"
VOLUME   = "vl_premium_forecast_tripwise"

EXP_NAME      = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
ARTIFACT_PATH = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/mlflow-artifacts"

if mlflow.get_experiment_by_name(EXP_NAME) is None:
    mlflow.create_experiment(name=EXP_NAME, artifact_location=ARTIFACT_PATH)
mlflow.set_experiment(EXP_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load feature table

# COMMAND ----------

features_df = spark.table("serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features").toPandas()
print(f"Loaded {len(features_df):,} rows")
features_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define features and targets

# COMMAND ----------

NUMERIC_FEATURES = [
    "current_driving_score",
    "total_trips",
    "total_miles",
    "avg_trip_score",
    "avg_trip_distance_miles",
    "avg_speed_mph",
    "avg_rapid_accel_count",
    "avg_sharp_turn_count",
    "avg_leaving_home_zone_count",
    "avg_rough_road_conditions_count",
    "avg_night_driving_pct",
    "avg_high_risk_area_pct",
    "avg_inclement_weather_pct",
    "embedded_conversion_rate",
]

CATEGORICAL_FEATURES = [
    "risk_tier",         # low / medium / high
    "state",             # NY, CA, TX, ...
    "coverage_level",    # basic / standard / premium
    "policy_type",       # annual / semi_annual / monthly
]

TARGET = "discount_pct"

ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# Drop rows with any nulls in features or target
df = features_df[ALL_FEATURES + [TARGET]].dropna()  # TARGET = discount_pct
print(f"Clean rows for training: {len(df):,}")

X = df[ALL_FEATURES]
y = df[TARGET]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train / test split

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"Train: {len(X_train):,}  |  Test: {len(X_test):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build preprocessing + model pipeline

# COMMAND ----------

preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), NUMERIC_FEATURES),
        (
            "cat",
            OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
            CATEGORICAL_FEATURES,
        ),
    ]
)

xgb_params = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
    "n_jobs": -1,
    "tree_method": "hist",
}

model_pipeline = Pipeline(
    steps=[
        ("preprocessor", preprocessor),
        ("regressor", XGBRegressor(**xgb_params)),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Train with MLflow tracking

# COMMAND ----------

with mlflow.start_run(run_name="xgboost_discount_v1") as run:

    # --- Train ---
    model_pipeline.fit(X_train, y_train)

    # --- Evaluate discount_pct ---
    discount_pred = model_pipeline.predict(X_test)

    mae = mean_absolute_error(y_test, discount_pred)
    r2  = r2_score(y_test, discount_pred)
    print(f"discount_pct  MAE={mae:.4f}  R²={r2:.4f}")

    # --- Log params & metrics ---
    mlflow.log_params({**xgb_params, "test_size": 0.2, "train_rows": len(X_train)})
    mlflow.log_metrics({
        "mae_discount_pct": round(mae, 4),
        "r2_discount_pct":  round(r2, 4),
    })

    # --- Log feature list as artifact ---
    import json, tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"numeric": NUMERIC_FEATURES, "categorical": CATEGORICAL_FEATURES}, f, indent=2)
        feature_manifest_path = f.name
    mlflow.log_artifact(feature_manifest_path, artifact_path="metadata")
    os.unlink(feature_manifest_path)

    # --- Define input example for serving ---
    input_example = X_test.iloc[:3]

    # --- Log & register model in Unity Catalog ---
    mlflow.sklearn.log_model(
        sk_model=model_pipeline,
        artifact_path="model",
        input_example=input_example,
        registered_model_name="serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model",
    )

    run_id = run.info.run_id
    print(f"\nRun ID: {run_id}")
    print(f"Registered: serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Promote the new version to 'Champion' alias
# MAGIC
# MAGIC UC model aliases let the serving endpoint always load `@champion`
# MAGIC without changing the endpoint config when you retrain.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

# Get the latest version just registered
model_name = "serverless_stable_mv5gxq_catalog.insurance_trip_wise.premium_forecast_model"
versions = client.search_model_versions(f"name='{model_name}'")
latest = sorted(versions, key=lambda v: int(v.version))[-1]

client.set_registered_model_alias(
    name=model_name,
    alias="champion",
    version=latest.version,
)

print(f"Model version {latest.version} promoted to alias 'champion'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quick smoke-test: load champion and predict on one driver

# COMMAND ----------

loaded_model = mlflow.sklearn.load_model(f"models:/{model_name}@champion")

sample             = X_test.iloc[[0]]
predicted_discount = loaded_model.predict(sample)[0]

print(f"\nSample driver features:")
print(sample[["current_driving_score", "risk_tier", "coverage_level", "total_miles"]].to_string(index=False))
print(f"\nPredicted discount : {predicted_discount:.1f}%")
print("(Adjusted premium is derived at serving time from the live policy premium)")
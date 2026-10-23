# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Premium Forecast — Feature Table Creation
# MAGIC
# MAGIC Builds `serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features` by joining:
# MAGIC - `dim_drivers`  — driving score, total trips/miles, risk tier, state
# MAGIC - `trips`        — per-driver aggregated behaviour (braking, speeding, night driving…)
# MAGIC - `policies`     — current active full-term policy (coverage level, policy type)
# MAGIC
# MAGIC Note: `premium_amount` and `discount_pct` are intentionally excluded — they are policy
# MAGIC state values (not ML features) and are fetched live from `policies` at inference time.
# MAGIC
# MAGIC This table is the single source of truth for both training and real-time inference.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load source tables

# COMMAND ----------

drivers = spark.table("serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers")
trips   = spark.table("serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips")
policies = spark.table("serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate trip-level behaviour per driver
# MAGIC
# MAGIC We want one row per driver capturing their overall driving behaviour.
# MAGIC Only completed trips (status = 'completed') are included.

# COMMAND ----------

trip_agg = (
    trips
    .filter(F.col("status") == "completed")
    .groupBy("device_id")
    .agg(
        F.count("trip_id").alias("trip_count"),
        F.round(F.avg("trip_score"), 2).alias("avg_trip_score"),
        F.round(F.avg("distance_miles"), 2).alias("avg_trip_distance_miles"),
        F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
        F.round(F.avg("rapid_accel_count"), 3).alias("avg_rapid_accel_count"),
        F.round(F.avg("sharp_turn_count"), 3).alias("avg_sharp_turn_count"),
        F.round(F.avg("leaving_home_zone_count"), 3).alias("avg_leaving_home_zone_count"),
        F.round(F.avg("rough_road_conditions_count"), 3).alias("avg_rough_road_conditions_count"),
        F.round(F.avg("night_driving_pct"), 4).alias("avg_night_driving_pct"),
        F.round(F.avg("high_risk_area_pct"), 4).alias("avg_high_risk_area_pct"),
        F.round(F.avg("inclement_weather_pct"), 4).alias("avg_inclement_weather_pct"),
        F.sum(F.col("did_convert").cast("int")).alias("total_embedded_conversions"),
        F.round(
            F.sum(F.col("did_convert").cast("int")) / F.count("trip_id"), 4
        ).alias("embedded_conversion_rate"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pull the current active full-term policy per driver
# MAGIC
# MAGIC We exclude `embedded_trip` policies — those are the per-trip add-ons.
# MAGIC The annual / full-term policy is the one whose premium and discount we want to predict.

# COMMAND ----------

# Keep only the most recent active full-term policy per driver
policy_window = Window.partitionBy("device_id").orderBy(F.col("effective_date").desc())

active_policy = (
    policies
    .filter(
        (F.col("is_active") == True) &
        (F.col("policy_type") != "embedded_trip")
    )
    .withColumn("rn", F.row_number().over(policy_window))
    .filter(F.col("rn") == 1)
    .select(
        "device_id",
        "policy_id",
        "coverage_level",
        "policy_type",
        "discount_pct",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join everything together

# COMMAND ----------

features = (
    drivers
    .filter(F.col("status") == "active")
    .select(
        "device_id",
        "current_driving_score",
        "total_trips",
        "total_miles",
        "risk_tier",
        "state",
        "has_embedded_policy",
    )
    .join(trip_agg, on="device_id", how="left")
    .join(active_policy, on="device_id", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Drop rows without a target (drivers with no active policy)

# COMMAND ----------

features = features.filter(F.col("discount_pct").isNotNull())

print(f"Feature table row count: {features.count()}")
features.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Unity Catalog

# COMMAND ----------

(
    features
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features")
)

print("Feature table written: serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick sanity check
# MAGIC SELECT
# MAGIC     risk_tier,
# MAGIC     coverage_level,
# MAGIC     COUNT(*)                             AS driver_count,
# MAGIC     ROUND(AVG(current_driving_score), 1) AS avg_score
# MAGIC FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features
# MAGIC GROUP BY risk_tier, coverage_level
# MAGIC ORDER BY risk_tier, coverage_level;
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Streaming Device Registration
# MAGIC Inserts devices into Lakebase `public.devices` at variable rates, simulating real-time registrations with bursts and dips across US-wide regions (matching load_sample_data_v4).
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `num_devices` (default: 1500) — total number of devices to register
# MAGIC - `profile` (default/demo_west) — workspace profile for Lakebase connection
# MAGIC
# MAGIC **Insert Pattern:**
# MAGIC - Normal: 1-10 devices per second
# MAGIC - Burst (~3-5s every ~15-20s): 8-15 devices per second
# MAGIC - Dip (~3-5s after burst): 0-2 devices per second
# MAGIC
# MAGIC **Lakebase Connection (per profile):**
# MAGIC - **default:** `ep-broad-frost-d1b4qdu7` / instance `DONOTDELETE-vibe-coding-workshop-lakebase` / `main.amee`
# MAGIC - **demo_west:** `instance-36870425-b394-49a5-a10d-6432f1945894` (provisioned) / schema `tripwise_demo` / `serverless_stable_mv5gxq_catalog.insurance_trip_wise`
# MAGIC - Auth: Dynamic OAuth via Databricks SDK

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --upgrade --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
import random
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Job parameters (configurable via widget / job params)
dbutils.widgets.text("num_devices", "1500", "Total devices to register")
dbutils.widgets.dropdown("profile", "fevm-target", ["fevm-target", "default", "demo_west"], "Workspace Profile")

NUM_DEVICES = int(dbutils.widgets.get("num_devices"))
PROFILE = dbutils.widgets.get("profile")

# Lakebase PostgreSQL connection details per profile
if PROFILE == "fevm-target":
    # Autoscaling Lakebase project 'tripwise' on fevm-target
    LAKEBASE_HOST = "ep-soft-frost-d263qfy8.database.us-east-1.cloud.databricks.com"
    LAKEBASE_DATABASE = "databricks_postgres"
    LAKEBASE_PORT = 5432
    LAKEBASE_USER = None  # set dynamically to the running principal below
    INSTANCE_NAME = None  # autoscaling: use the postgres credential API (not w.database)
    ENDPOINT_PATH = "projects/tripwise/branches/production/endpoints/primary"
    LAKEBASE_SCHEMA = "tripwise_demo"
    CATALOG_SCHEMA = "serverless_stable_mv5gxq_catalog.insurance_trip_wise"
elif PROFILE == "demo_west":
    LAKEBASE_HOST = "ep-soft-frost-d263qfy8.database.us-east-1.cloud.databricks.com"
    LAKEBASE_DATABASE = "databricks_postgres"
    LAKEBASE_PORT = 5432
    LAKEBASE_USER = "amee.vora@databricks.com"
    INSTANCE_NAME = None
    ENDPOINT_PATH = "projects/tripwise/branches/production/endpoints/primary"
    LAKEBASE_SCHEMA = "tripwise_demo"
    CATALOG_SCHEMA = "serverless_stable_mv5gxq_catalog.insurance_trip_wise"
else:
    LAKEBASE_HOST = "ep-broad-frost-d1b4qdu7.database.us-west-2.cloud.databricks.com"
    LAKEBASE_DATABASE = "databricks_postgres"
    LAKEBASE_PORT = 5432
    LAKEBASE_USER = "amee.vora@databricks.com"
    INSTANCE_NAME = "DONOTDELETE-vibe-coding-workshop-lakebase"
    ENDPOINT_PATH = None
    LAKEBASE_SCHEMA = "public"
    CATALOG_SCHEMA = "main.amee"

print(f"Profile: {PROFILE}")
print(f"Lakebase Host: {LAKEBASE_HOST}")
print(f"Lakebase Database: {LAKEBASE_DATABASE}")
print(f"Lakebase User: {LAKEBASE_USER}")
print(f"Instance Name: {INSTANCE_NAME}")
print(f"Catalog/Schema: {CATALOG_SCHEMA}")
print(f"Total devices to register: {NUM_DEVICES}")

# COMMAND ----------

# US-wide registration locations (matches load_sample_data_v4)
US_REGIONS = {
    "nyc": [
        (40.7580, -73.9855), (40.7484, -73.9857), (40.7061, -74.0089),
        (40.7282, -73.7949), (40.6501, -73.9496), (40.8448, -73.8648),
    ],
    "boston": [
        (42.3601, -71.0589), (42.3467, -71.0972), (42.3736, -71.1097),
        (42.3554, -71.0640),
    ],
    "philadelphia": [
        (39.9526, -75.1652), (39.9482, -75.1718), (39.9496, -75.1503),
        (39.9012, -75.1720),
    ],
    "dc": [
        (38.8977, -77.0365), (38.8899, -77.0091), (38.9072, -77.0369),
        (38.9217, -77.0422),
    ],
    "miami": [
        (25.7617, -80.1918), (25.7907, -80.1300), (25.8103, -80.1901),
        (25.7260, -80.2410),
    ],
    "atlanta": [
        (33.7490, -84.3880), (33.7756, -84.2963), (33.8486, -84.3733),
        (33.7577, -84.3963),
    ],
    "chicago": [
        (41.8781, -87.6298), (41.8827, -87.6233), (41.9484, -87.6553),
        (41.8119, -87.6980), (41.9032, -87.6267),
    ],
    "dallas": [
        (32.7767, -96.7970), (32.7555, -96.8098), (32.8143, -96.9489),
        (32.9483, -96.7299),
    ],
    "houston": [
        (29.7604, -95.3698), (29.7355, -95.3885), (29.7519, -95.3584),
        (29.7866, -95.3909),
    ],
    "phoenix": [
        (33.4484, -112.0740), (33.5092, -111.8985), (33.4152, -111.8315),
        (33.4373, -112.0078),
    ],
    "denver": [
        (39.7392, -104.9903), (39.7508, -104.9966), (39.7633, -104.9994),
        (39.6880, -104.9686),
    ],
    "seattle": [
        (47.6062, -122.3321), (47.6205, -122.3493), (47.6145, -122.3440),
        (47.6535, -122.3470),
    ],
    "la": [
        (34.0522, -118.2437), (34.0195, -118.4912), (34.0928, -118.3287),
        (33.9425, -118.4081),
    ],
    "sf": [
        (37.7749, -122.4194), (37.8083, -122.4156), (37.7599, -122.4148),
        (37.7849, -122.4094),
    ],
}

REGION_NAMES = list(US_REGIONS.keys())
# Weights match load_sample_data_v4 REGION_DRIVER_WEIGHTS
REGION_WEIGHTS = [0.12, 0.05, 0.05, 0.06, 0.08, 0.07, 0.10, 0.08, 0.08, 0.06, 0.05, 0.05, 0.10, 0.05]

FIRST_NAMES = [
    "Liam", "Olivia", "Noah", "Emma", "Aiden", "Ava", "Jackson", "Sophia",
    "Sebastian", "Isabella", "Mateo", "Mia", "Henry", "Charlotte", "Owen",
    "Amelia", "Alexander", "Harper", "Luca", "Evelyn", "Benjamin", "Luna",
    "Theodore", "Ella", "Leo", "Gianna", "Jack", "Aurora", "Miles", "Penelope",
    "Ezra", "Layla", "Daniel", "Nora", "Kai", "Hazel", "Caleb", "Ivy",
    "Wyatt", "Willow", "Jaxon", "Stella", "Asher", "Violet", "Carter",
    "Nova", "Lincoln", "Ellie", "Hudson", "Scarlett",
]

LAST_NAMES = [
    "Rivera", "Chang", "Patel", "Kim", "Nguyen", "Okafor", "Tanaka", "Singh",
    "Costa", "Johansson", "Mueller", "Ivanov", "Dubois", "Fernandez", "Ali",
    "Park", "Nakamura", "Santos", "Andersen", "Volkov", "Chen", "Moreau",
    "Gupta", "Yamamoto", "Schmidt", "Petrov", "Larsson", "Rossi", "Takahashi",
    "Reyes", "Hoffman", "Sato", "Kowalski", "Bergman", "Fischer", "Bianchi",
    "Olsen", "Suzuki", "Delgado", "Mayer", "Hayashi", "Ortega", "Wagner",
    "Kimura", "Mendez", "Lindberg", "Ito", "Vargas", "Becker", "Shimizu",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate OAuth Token & Connect to Lakebase

# COMMAND ----------

w = WorkspaceClient()
print("Generating Lakebase OAuth token...")
if INSTANCE_NAME:
    # Provisioned instance
    cred = w.database.generate_database_credential(instance_names=[INSTANCE_NAME])
    token = cred.token
else:
    # Autoscaling instance - use the postgres credential API
    cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT_PATH})
    token = cred["token"]
# Default the DB user to the running principal when not pinned in the profile config
if not LAKEBASE_USER:
    LAKEBASE_USER = w.current_user.me().user_name
print("Token generated successfully.")

print(f"Connecting to Lakebase at {LAKEBASE_HOST}:{LAKEBASE_PORT}/{LAKEBASE_DATABASE} as {LAKEBASE_USER}...")
conn = psycopg2.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    database=LAKEBASE_DATABASE,
    user=LAKEBASE_USER,
    password=token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()
print("Connected to Lakebase PostgreSQL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-detect start index

# COMMAND ----------

cur.execute(f"SELECT COALESCE(MAX(CAST(SUBSTRING(device_id FROM 4) AS INTEGER)), 0) FROM {LAKEBASE_SCHEMA}.devices")
max_index = cur.fetchone()[0]
START_INDEX = max_index + 1
print(f"Current max device index: {max_index}")
print(f"Starting new registrations at DEV{START_INDEX:04d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert Devices (variable rate with bursts and dips)

# COMMAND ----------

rng = random.Random(int(time.time()))

total_inserted = 0
second_count = 0

# State machine: "normal", "burst", "dip"
mode = "normal"
mode_seconds_left = rng.randint(10, 15)  # seconds until first burst

print(f"Registering {NUM_DEVICES} devices starting at DEV{START_INDEX:04d}...")
print(f"Variable rate: normal (1-10/s), burst (8-15/s), dip (0-2/s)\n")

while total_inserted < NUM_DEVICES:
    # Determine batch size based on current mode
    if mode == "normal":
        batch_size = rng.randint(1, 10)
    elif mode == "burst":
        batch_size = rng.randint(8, 15)
    else:  # dip
        batch_size = rng.randint(0, 2)

    # Don't exceed total
    batch_size = min(batch_size, NUM_DEVICES - total_inserted)

    # Insert the batch for this second
    for j in range(batch_size):
        idx = START_INDEX + total_inserted
        device_id = f"DEV{idx:04d}"

        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        driver_name = f"{first} {last}"

        region = rng.choices(REGION_NAMES, weights=REGION_WEIGHTS, k=1)[0]
        locs = US_REGIONS[region]
        base_loc = rng.choice(locs)
        reg_lat = base_loc[0] + rng.uniform(-0.015, 0.015)
        reg_lon = base_loc[1] + rng.uniform(-0.015, 0.015)

        now = datetime.utcnow()

        cur.execute(
            f"""
            INSERT INTO {LAKEBASE_SCHEMA}.devices
            (device_id, driver_name, registered_at, last_active, timestamp,
             registration_latitude, registration_longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id) DO NOTHING
            """,
            (device_id, driver_name, now, now, now, reg_lat, reg_lon),
        )

        total_inserted += 1

    second_count += 1
    mode_label = f"[{mode.upper():6s}]"
    print(f"{mode_label} Second {second_count}: inserted {batch_size} devices "
          f"(total: {total_inserted}/{NUM_DEVICES})")

    # Transition mode
    mode_seconds_left -= 1
    if mode_seconds_left <= 0:
        if mode == "normal":
            mode = "burst"
            mode_seconds_left = rng.randint(3, 5)
        elif mode == "burst":
            mode = "dip"
            mode_seconds_left = rng.randint(3, 5)
        else:  # dip -> normal
            mode = "normal"
            mode_seconds_left = rng.randint(10, 15)

    # Wait 1 second between batches (skip on last iteration)
    if total_inserted < NUM_DEVICES:
        time.sleep(1)

cur.close()
conn.close()

print(f"\nDone! Registered {total_inserted} devices "
      f"(DEV{START_INDEX:04d} - DEV{START_INDEX + total_inserted - 1:04d}) "
      f"over {second_count} seconds")
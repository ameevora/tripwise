# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Zerobus Telemetry Ingestion
# MAGIC Ingests simulated telematics data into Delta table `serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data` via Zerobus streaming.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `num_records` (default: 5000) — total number of telemetry records to ingest (distributed across ALL selected devices)
# MAGIC - `num_devices` (default: 1000) — number of devices to generate telemetry for (drawn from Lakebase, capped to the registered count)
# MAGIC - `num_days` (default: 10) — number of days to spread trips across
# MAGIC
# MAGIC **Device IDs:** pulled live from Lakebase `tripwise_demo.devices` (first `num_devices`, ordered) during setup, so every telemetry `device_id` is guaranteed to exist in the registration table — telemetry always joins to devices.
# MAGIC
# MAGIC **Record distribution:** production-like heavy-tail — a minority of devices generate most telemetry (Pareto-weighted), many are light users, but every device gets a 1-record baseline so all appear and still join. Totals sum to exactly `num_records`. Requires `num_records >= num_devices` for full coverage.
# MAGIC
# MAGIC **Trip generation:**
# MAGIC - Trip IDs use `TRIP{counter:05d}` format matching load_data_v4
# MAGIC - Locations drawn from US_REGIONS matching agg_location states
# MAGIC - Coordinates interpolated along trip path between start/end locations
# MAGIC - Time-of-day: peak morning (7-9am), regular noon (11am-2pm), dip at night (10pm-5am)
# MAGIC
# MAGIC **Zerobus Connection:**
# MAGIC - Endpoint: Databricks Zerobus streaming ingest
# MAGIC - Table: `serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data`
# MAGIC - Auth: Service principal OAuth (client credentials)

# COMMAND ----------

# MAGIC %pip install "databricks-zerobus-ingest-sdk==0.2.0" nest_asyncio psycopg2-binary databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import asyncio
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import psycopg2
from databricks.sdk import WorkspaceClient

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("num_records", "5000", "Total telemetry records")
dbutils.widgets.text("num_devices", "1000", "Devices to cover (from Lakebase; capped to registered count)")
dbutils.widgets.text("num_days", "10", "Number of days to spread trips")

NUM_RECORDS = int(dbutils.widgets.get("num_records"))
NUM_DEVICES = int(dbutils.widgets.get("num_devices"))
NUM_DAYS = int(dbutils.widgets.get("num_days"))

# Zerobus configuration — retrieve from Databricks secrets
INGEST_URL = dbutils.secrets.get(scope="zerobus", key="ingest_url")
WORKSPACE_URL = dbutils.secrets.get(scope="zerobus", key="workspace_url")
CLIENT_ID = dbutils.secrets.get(scope="zerobus", key="client_id")
CLIENT_SECRET = dbutils.secrets.get(scope="zerobus", key="client_secret")

TABLE_NAME = "serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data"

# Lakebase (autoscaling project 'tripwise') — source of truth for device IDs, so
# telemetry device_ids are always a subset of the registered devices and every
# record joins to tripwise_demo.devices.
LAKEBASE_HOST = "ep-soft-frost-d263qfy8.database.us-east-1.cloud.databricks.com"
LAKEBASE_DATABASE = "databricks_postgres"
LAKEBASE_PORT = 5432
LAKEBASE_ENDPOINT_PATH = "projects/tripwise/branches/production/endpoints/primary"
LAKEBASE_SCHEMA = "tripwise_demo"

print(f"Zerobus endpoint: {INGEST_URL}")
print(f"Workspace: {WORKSPACE_URL}")
print(f"Target table: {TABLE_NAME}")
print(f"Records: {NUM_RECORDS}, Devices: {NUM_DEVICES}, Days: {NUM_DAYS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Registered Device IDs from Lakebase
# MAGIC Pull the real device IDs from Lakebase `tripwise_demo.devices` and draw the telemetry
# MAGIC subset (first `num_devices`, ordered) from that set. This guarantees every telemetry
# MAGIC `device_id` exists in the registration table, so telemetry always joins to devices.
# MAGIC Runs during setup — adds only a single small SELECT, no cost to the streaming ingest.

# COMMAND ----------

# Authenticate as the SAME service principal the Zerobus ingest uses — client_id /
# client_secret from the 'zerobus' secret scope — so the whole notebook runs under one
# identity and this works in a job (not just interactively). The SP's client_id is also
# its Lakebase Postgres role name; it must have a federated LAKEBASE_OAUTH_V1 role on the
# branch (projects/tripwise/branches/production). Read-only here.
w = WorkspaceClient(host=WORKSPACE_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
_cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": LAKEBASE_ENDPOINT_PATH})

_lb_conn = psycopg2.connect(
    host=LAKEBASE_HOST,
    port=LAKEBASE_PORT,
    database=LAKEBASE_DATABASE,
    user=CLIENT_ID,
    password=_cred["token"],
    sslmode="require",
)
_lb_cur = _lb_conn.cursor()
_lb_cur.execute(f"SELECT device_id FROM {LAKEBASE_SCHEMA}.devices ORDER BY device_id")
registered_device_ids = [r[0] for r in _lb_cur.fetchall()]
_lb_cur.close()
_lb_conn.close()

if not registered_device_ids:
    raise RuntimeError(
        f"No devices found in Lakebase {LAKEBASE_SCHEMA}.devices — run "
        "register_devices_streaming first so telemetry has devices to join to."
    )

# Draw the telemetry subset from the registered set (first-N, ordered). Cap to the
# registered count so we can never ask for more devices than exist.
num = min(NUM_DEVICES, len(registered_device_ids))
selected_device_ids = registered_device_ids[:num]

# Tie check — true by construction; assert + report so the demo can show 100% join.
assert set(selected_device_ids).issubset(set(registered_device_ids))
print(f"Lakebase registered devices: {len(registered_device_ids)} "
      f"({registered_device_ids[0]} - {registered_device_ids[-1]})")
print(f"Telemetry subset: {len(selected_device_ids)} devices "
      f"({selected_device_ids[0]} - {selected_device_ids[-1]}) — all join to tripwise_demo.devices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locations & Trip Planning

# COMMAND ----------

# US regions matching agg_location states (from load_data_v4)
US_REGIONS = {
    "nyc": [
        (40.7580, -73.9855, "Times Square"), (40.7484, -73.9857, "Empire State"),
        (40.7061, -74.0089, "Wall Street"), (40.7282, -73.7949, "Queens"),
        (40.6501, -73.9496, "Brooklyn"), (40.8448, -73.8648, "Bronx"),
    ],
    "boston": [
        (42.3601, -71.0589, "Boston Common"), (42.3467, -71.0972, "Fenway Park"),
        (42.3736, -71.1097, "Harvard Square"), (42.3554, -71.0640, "Faneuil Hall"),
    ],
    "philadelphia": [
        (39.9526, -75.1652, "City Hall"), (39.9482, -75.1718, "Rittenhouse Sq"),
        (39.9496, -75.1503, "Old City"), (39.9012, -75.1720, "South Philly"),
    ],
    "dc": [
        (38.8977, -77.0365, "White House"), (38.8899, -77.0091, "US Capitol"),
        (38.9072, -77.0369, "Dupont Circle"), (38.9217, -77.0422, "Adams Morgan"),
    ],
    "miami": [
        (25.7617, -80.1918, "Downtown Miami"), (25.7907, -80.1300, "Miami Beach"),
        (25.8103, -80.1901, "Wynwood"), (25.7260, -80.2410, "Coral Gables"),
    ],
    "atlanta": [
        (33.7490, -84.3880, "Downtown Atlanta"), (33.7756, -84.2963, "Decatur"),
        (33.8486, -84.3733, "Buckhead"), (33.7577, -84.3963, "Midtown"),
    ],
    "chicago": [
        (41.8781, -87.6298, "The Loop"), (41.8827, -87.6233, "Millennium Park"),
        (41.9484, -87.6553, "Wrigleyville"), (41.8119, -87.6980, "Midway"),
        (41.9032, -87.6267, "Lincoln Park"),
    ],
    "dallas": [
        (32.7767, -96.7970, "Downtown Dallas"), (32.7555, -96.8098, "Deep Ellum"),
        (32.8143, -96.9489, "Irving"), (32.9483, -96.7299, "Plano"),
    ],
    "houston": [
        (29.7604, -95.3698, "Downtown Houston"), (29.7355, -95.3885, "Museum District"),
        (29.7519, -95.3584, "Midtown"), (29.7866, -95.3909, "Heights"),
    ],
    "phoenix": [
        (33.4484, -112.0740, "Downtown Phoenix"), (33.5092, -111.8985, "Scottsdale"),
        (33.4152, -111.8315, "Tempe"), (33.4373, -112.0078, "Central Ave"),
    ],
    "denver": [
        (39.7392, -104.9903, "Downtown Denver"), (39.7508, -104.9966, "LoDo"),
        (39.7633, -104.9994, "RiNo"), (39.6880, -104.9686, "Cherry Creek"),
    ],
    "seattle": [
        (47.6062, -122.3321, "Downtown Seattle"), (47.6205, -122.3493, "Space Needle"),
        (47.6145, -122.3440, "South Lake Union"), (47.6535, -122.3470, "Fremont"),
    ],
    "la": [
        (34.0522, -118.2437, "Downtown LA"), (34.0195, -118.4912, "Santa Monica"),
        (34.0928, -118.3287, "Hollywood"), (33.9425, -118.4081, "LAX Area"),
    ],
    "sf": [
        (37.7749, -122.4194, "Downtown SF"), (37.8083, -122.4156, "Fishermans Wharf"),
        (37.7599, -122.4148, "Mission District"), (37.7849, -122.4094, "SoMa"),
    ],
}

# Region weights matching agg_location trip_cnt distribution (Texas heaviest, then NY, CA, IL)
REGION_WEIGHTS = {
    "dallas": 0.10, "houston": 0.10,   # Texas — highest in agg_location
    "nyc": 0.12,                        # New York
    "la": 0.07, "sf": 0.07,            # California
    "chicago": 0.10,                    # Illinois
    "miami": 0.08,                      # Florida
    "atlanta": 0.07,                    # Georgia
    "denver": 0.06,                     # Colorado
    "phoenix": 0.06,                    # Arizona
    "dc": 0.05,                         # DC
    "boston": 0.05,                      # Massachusetts
    "philadelphia": 0.04,               # Pennsylvania
    "seattle": 0.03,                    # Washington
}

def interpolate_location(start_lat, start_lon, end_lat, end_lon, pct):
    lat = start_lat + (end_lat - start_lat) * pct + random.uniform(-0.002, 0.002)
    lon = start_lon + (end_lon - start_lon) * pct + random.uniform(-0.002, 0.002)
    return (lat, lon)

def pick_trip_hour():
    """Pick hour of day with realistic driving distribution.
    Peak: 7-9am (morning commute), regular: 11am-2pm, dip: 10pm-5am."""
    # Hourly weights — index = hour of day
    weights = [
        1, 1, 0.5, 0.5, 0.5, 1,      # 0-5am: night dip
        3, 8, 9, 6, 5, 6,             # 6-11am: morning ramp + peak at 7-8
        7, 7, 6, 5, 6, 8,             # 12-5pm: regular afternoon + evening commute start
        7, 5, 4, 3, 2, 1,             # 6-11pm: evening taper
    ]
    hour = random.choices(range(24), weights=weights, k=1)[0]
    return hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Trip Plan

# COMMAND ----------

# Pre-generate all trips across devices and days
random.seed(42)
base_date = datetime(2026, 3, 1)

region_names = list(REGION_WEIGHTS.keys())
region_wts = [REGION_WEIGHTS[r] for r in region_names]

# Assign each device a home region (device IDs come from Lakebase — see setup cell)
device_regions = {}
for device_id in selected_device_ids:
    device_regions[device_id] = random.choices(region_names, weights=region_wts, k=1)[0]

# Per-device record budget — PRODUCTION-LIKE heavy-tail distribution.
# Real telematics volume is highly skewed: a minority of active drivers generate most of
# the data, many are light users, a few are barely active. We model that instead of an
# even split: give every device a 1-record baseline (so all devices appear and still join
# to tripwise_demo.devices), then allocate the remaining budget using heavy-tailed (Pareto)
# activity weights — roughly ~20% of devices get ~80% of the extra volume. Totals still
# sum to exactly NUM_RECORDS.
PARETO_ALPHA = 1.3  # lower = heavier tail (more extreme power users)

n_dev = len(selected_device_ids)
baseline = 1 if NUM_RECORDS >= n_dev else 0
if NUM_RECORDS < n_dev:
    print(f"WARNING: num_records ({NUM_RECORDS}) < devices ({n_dev}); not every device can "
          f"get a record. Raise num_records to >= {n_dev} for full coverage.")

records_budget = {device_id: baseline for device_id in selected_device_ids}
remaining_budget = NUM_RECORDS - baseline * n_dev

# Heavy-tail activity weight per device; allocate the remaining records by weighted draw.
activity_weights = [random.paretovariate(PARETO_ALPHA) for _ in selected_device_ids]
for idx in random.choices(range(n_dev), weights=activity_weights, k=max(0, remaining_budget)):
    records_budget[selected_device_ids[idx]] += 1

# Plan trips to fill each device's budget. A device's records are split into trips of
# 5-20 records (capped by remaining budget): a tight budget collapses to one short trip,
# a larger budget produces several realistic trips spread across the day window.
trips = []
trip_counter = 1

for device_id in selected_device_ids:
    region = device_regions[device_id]
    locs = US_REGIONS[region]
    remaining = records_budget[device_id]

    while remaining > 0:
        rec = min(remaining, random.randint(5, 20))
        remaining -= rec

        trip_id = f"TRIP{trip_counter:05d}"
        trip_counter += 1

        start_loc = random.choice(locs)
        end_loc = random.choice(locs)
        while end_loc == start_loc and len(locs) > 1:
            end_loc = random.choice(locs)

        day = random.randint(0, NUM_DAYS - 1)
        hour = pick_trip_hour()
        minute = random.randint(0, 59)
        trip_start_time = (base_date + timedelta(days=day)).replace(hour=hour, minute=minute, second=0)

        trips.append({
            "device_id": device_id,
            "trip_id": trip_id,
            "start_loc": start_loc,
            "end_loc": end_loc,
            "trip_start_time": trip_start_time,
            "records_per_trip": rec,
            "is_night": hour >= 22 or hour < 5,
        })

# Totals are exact by construction: each device's trips sum to its budget, and the
# budgets sum to NUM_RECORDS — no rescaling or flooring needed.
total_records = sum(t["records_per_trip"] for t in trips)
devices_covered = len({t["device_id"] for t in trips})
_vols = sorted(records_budget.values())
_p50 = _vols[len(_vols) // 2]
_p90 = _vols[int(len(_vols) * 0.9)]
_heavy = sum(1 for v in _vols if v >= _p90)
print(f"Planned {len(trips)} trips for {devices_covered}/{n_dev} devices across {NUM_DAYS} days")
print(f"Total records to ingest: {total_records} (target {NUM_RECORDS})")
print(f"Per-device volume (heavy-tail): min {_vols[0]}, median {_p50}, p90 {_p90}, max {_vols[-1]}")
print(f"  top {_heavy} devices (>= p90) drive the bulk of telemetry — production-like skew")
print(f"Trip IDs: TRIP00001 - TRIP{trip_counter - 1:05d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record Generator

# COMMAND ----------

def generate_telemetry_record(trip, record_idx, total_in_trip):
    """Generate a telemetry record interpolated along trip path."""
    pct = record_idx / max(1, total_in_trip - 1)
    lat, lon = interpolate_location(
        trip["start_loc"][0], trip["start_loc"][1],
        trip["end_loc"][0], trip["end_loc"][1],
        pct,
    )

    ts = trip["trip_start_time"] + timedelta(seconds=record_idx * 10)

    event_roll = random.random()
    if event_roll < 0.70:
        event_type = "normal"
        event_severity = random.uniform(0.0, 0.2)
    elif event_roll < 0.80:
        event_type = "hard_brake"
        event_severity = random.uniform(0.4, 0.8)
    elif event_roll < 0.88:
        event_type = "rapid_accel"
        event_severity = random.uniform(0.3, 0.7)
    elif event_roll < 0.94:
        event_type = "sharp_turn"
        event_severity = random.uniform(0.5, 0.9)
    else:
        event_type = "speeding"
        event_severity = random.uniform(0.6, 1.0)

    return {
        "device_id": trip["device_id"],
        "timestamp": int(ts.timestamp()),
        "latitude": lat,
        "longitude": lon,
        "altitude": random.uniform(150, 300),
        "speed_mph": random.uniform(0, 75) if event_type != "speeding" else random.uniform(75, 95),
        "acceleration_x": random.uniform(-0.3, 0.5) if event_type == "normal" else random.uniform(-1.0, 1.0),
        "acceleration_y": random.uniform(-0.2, 0.2) if event_type != "sharp_turn" else random.uniform(-0.8, 0.8),
        "acceleration_z": random.uniform(-0.1, 0.1),
        "heading": random.uniform(0, 360),
        "event_type": event_type,
        "event_severity": round(event_severity, 2),
        "trip_id": trip["trip_id"],
        "is_night_driving": trip["is_night"],
        "phone_usage": random.random() < 0.05,
        "trip_distance_miles": round(pct * random.uniform(3, 25), 2),
        "trip_duration_seconds": record_idx * 10,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest via Zerobus

# COMMAND ----------

table_properties = TableProperties(TABLE_NAME)

async def ingest():
    sdk = ZerobusSdk(INGEST_URL, WORKSPACE_URL)

    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        max_inflight_records=50000,
        recovery=True,
        ack_callback=lambda r: None,
    )

    stream = await sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

    futures = []
    ingested = 0

    try:
        for trip in trips:
            n = trip["records_per_trip"]
            for j in range(n):
                record = generate_telemetry_record(trip, j, n)
                future = await stream.ingest_record(record)
                futures.append(future)
                ingested += 1

            if ingested % 500 == 0 or ingested == total_records:
                print(f"Queued {ingested}/{total_records} records ({len(trips)} trips)")

        await stream.flush()
        await asyncio.gather(*futures)

        print(f"\nSuccessfully ingested {ingested} records to {TABLE_NAME}")
        print(f"Devices: {selected_device_ids[0]} - {selected_device_ids[-1]} "
              f"({len(selected_device_ids)} devices, all registered in Lakebase)")
        print(f"Trips: TRIP00001 - TRIP{trip_counter - 1:05d}")

    finally:
        await stream.close()

import nest_asyncio
nest_asyncio.apply()
asyncio.run(ingest())
# Execution Evidence — Lakeflow Declarative Pipeline (data-quality expectations)

Component: **Trip ↔ telemetry enrichment pipeline** — `TripWise - Trip Telemetry
Enrichment (Lakeflow Declarative Pipeline)`, pipeline ID `152ff589-729d-42c5-b872-4d0a699ab07e`.
Closes the Gap 1 finding ("ingestion/transformation built from custom streaming code
and notebook jobs rather than a declarative Lakeflow pipeline with built-in
data-quality expectations") for the trip/telemetry enrichment layer specifically.

## Pipeline shape — 3 hops, SQL only

```
01_bronze_ingest.sql    bronze_trips, bronze_telemetry       — raw passthrough, no expectations
02_silver_quality.sql   silver_trips, silver_telemetry        — expectations enforced here
03_gold_enriched.sql    trip_telemetry_enriched (MV)          — last-record-per-trip join only
```

Bronze streams the two pre-existing source tables (`trips`,
`brickwise_device_data`) into pipeline-owned tables. Silver attaches 15
`CONSTRAINT ... EXPECT` rules — join-key fields `ON VIOLATION DROP ROW`, range/consistency
checks tracked without dropping. Gold is a pure join: dedupes `silver_telemetry` to the
last record per `trip_id` (`ROW_NUMBER() ... ORDER BY timestamp DESC`), then joins onto
`silver_trips` — one row per trip, no fan-out.

**Known data issue found and worked around:** `trips` and `brickwise_device_data` were
generated independently and do not share a consistent `trip_id → device_id` mapping
(same `trip_id`, different `device_id` on each side). Joining on `device_id AND trip_id`
together dropped all but 23 of 10,000 trips. Fixed by joining on `trip_id` only and
taking `device_id` from `trips` (the canonical source) — see `03_gold_enriched.sql`
comments.

## Run output — full-refresh update `803558a8-e892-4877-90c1-7702bfb21aef`

```
update_id: 803558a8-e892-4877-90c1-7702bfb21aef
state: COMPLETED
```

### Data-quality expectations (from `event_log()`, this update)

```
silver_telemetry — 125,717 records, 0 dropped, 0 warned
  trip_id_not_null         passed 125717 / failed 0
  device_id_not_null       passed 125717 / failed 0
  valid_event_type         passed 125717 / failed 0
  valid_is_night_driving   passed 125717 / failed 0
  valid_phone_usage        passed 125717 / failed 0

silver_trips — 10,000 records, 0 dropped, 0 warned
  trip_id_not_null         passed 10000 / failed 0
  device_id_not_null       passed 10000 / failed 0
  valid_trip_score         passed 10000 / failed 0
  non_negative_distance    passed 10000 / failed 0
  positive_duration        passed 10000 / failed 0
  valid_speed_consistency  passed 10000 / failed 0
  valid_night_pct          passed 10000 / failed 0
  valid_risk_pct           passed 10000 / failed 0
  valid_weather_pct        passed 10000 / failed 0
  offers_ticks_tie         passed 10000 / failed 0
  chronological_trip       passed 10000 / failed 0

trip_telemetry_enriched (gold MV) — status COMPLETED, 0 dropped, 0 warned
```

All 15 expectations across both silver tables passed at 100% — real synthetic data,
no DQ violations.

### Gold output verification

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT trip_id) AS distinct_trips,
       COUNT(DISTINCT device_id) AS distinct_devices,
       COUNT(event_type) AS non_null_event_type,
       COUNT(is_night_driving) AS non_null_night,
       COUNT(phone_usage) AS non_null_phone
FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.trip_telemetry_enriched;
```
```
 rows  | distinct_trips | distinct_devices | non_null_event_type | non_null_night | non_null_phone
-------+----------------+------------------+----------------------+----------------+----------------
 10000 |          10000 |             1500 |                10000 |          10000 |          10000
```

10,000 rows, exactly 10,000 distinct trips (one row per trip, no fan-out), all three
enriched telemetry fields fully populated.

## How to reproduce

```bash
# Sync SQL + create pipeline (one-time)
databricks workspace import /Workspace/Users/<user>/tripwise/pipelines/transformations/01_bronze_ingest \
  --file tripwise/pipelines/transformations/01_bronze_ingest.sql --language SQL --format SOURCE --overwrite -p fevm-target
databricks pipelines create --json @tripwise/pipelines/tripwise_telemetry_pipeline.json -p fevm-target

# Run
databricks pipelines start-update <pipeline_id> --full-refresh -p fevm-target

# Check DQ metrics for the most recent update
databricks api post /api/2.0/sql/statements -p fevm-target --json '{
  "warehouse_id": "<warehouse_id>",
  "statement": "SELECT details:flow_progress:status AS status, details:flow_progress:data_quality AS dq, origin.flow_name AS flow_name FROM event_log(\"<pipeline_id>\") WHERE event_type = \"flow_progress\" AND details:flow_progress:data_quality IS NOT NULL"
}'
```

## What this does and does not close

**Closes:** a real declarative Lakeflow pipeline now exists, with built-in,
UC-lineage-tracked data-quality expectations, for the trip ↔ telemetry enrichment
layer — exactly the reviewer's literal ask.

**Does not close:** the Zerobus streaming ingest notebook
(`notebooks/ingest_telemetry_zerobus.py`) still lands `brickwise_device_data` outside
this pipeline (by design — Zerobus's value is sub-second streaming ingest, not
pipeline orchestration), and `notebooks/create_feature_table.py` (builds
`ml_premium_features`) still runs as a separate custom notebook job, untouched by this
pipeline.

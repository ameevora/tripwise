# Execution Evidence — Ingest: Zerobus (Lakeflow Connect)

Component: **Lakeflow Connect / Zerobus** — telemetry ingestion.
Notebook: `tripwise/notebooks/ingest_telemetry_zerobus` (run by job *Device Telemetry
Data*, `420962304813966`). Full run with outputs:
[`notebooks/ingest_telemetry_zerobus.ipynb`](notebooks/ingest_telemetry_zerobus.ipynb).

## Committed notebook output (text, from the `.ipynb`)
```
Target table: serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data
Records: 5000, Devices: 1000, Days: 10
Lakebase registered devices: 1000 (DEV0001 - DEV1000)
Telemetry subset: 1000 devices (DEV0001 - DEV1000) — all join to tripwise_demo.devices
Planned 1143 trips for 1000/1000 devices across 10 days
Total records to ingest: 5000 (target 5000)
Per-device volume (heavy-tail): min 1, median 3, p90 8, max 454
Queued 5000/5000 records (1143 trips)
Successfully ingested 5000 records to serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data
Devices: DEV0001 - DEV1000 (1000 devices, all registered in Lakebase)
```
(Workspace/ingest URLs scrubbed to `<redacted>` in the committed notebook.)

## Corroborating table state (Delta target)
`serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data`:
```
records | devices | trips
--------+---------+------
 11101  |  1000   | 1144
```
All 1000 device_ids present → every telemetry row joins to `tripwise_demo.devices`
(the Lakebase↔Lakehouse "tie"). Record count exceeds a single 5000-run because multiple
ingests accumulated.

## How to reproduce
```bash
databricks api post /api/2.0/sql/statements -p fevm-target --json '{
 "warehouse_id":"22ad910fe1aed92d",
 "statement":"SELECT count(*) records, count(DISTINCT device_id) devices, count(DISTINCT trip_id) trips FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data"}'
```

> Telemetry is fully synthetic (generated in the notebook from Lakebase device IDs) — no customer data.

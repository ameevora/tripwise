# Notebooks & Jobs

> **Ingestion:** telemetry is ingested via **Zerobus** — the direct-write streaming
> ingestion API within **Lakeflow Connect** (`ingest_telemetry_zerobus`, run by the
> **Device Telemetry Data** job). Device registrations stream into Lakebase via
> `register_devices_streaming`. All orchestrated as Lakeflow Jobs.

## 1. `register_devices_streaming` — 1000 → 1500 devices
Changed the default device count from 1000 to **1500**.

- File (workspace, what the job runs):
  `/Workspace/Users/amee.vora@databricks.com/tripwise/notebooks/register_devices_streaming`
- File (local): `tripwise/notebooks/register_devices_streaming.py`
- Edits:
  - Widget default: `dbutils.widgets.text("num_devices", "1500", "Total devices to register")`
  - Doc comment: `num_devices (default: 1500) — total number of devices to register`

> **Behavior note:** the notebook auto-detects the start index via
> `SELECT MAX(device_id) FROM devices` and registers `num_devices` **more** starting
> at `max+1` — it appends, it does not cap the table at 1500. Run
> `truncate_devices_lakebase` first if an exact total is required.

## 2. Registration job — pinned `num_devices=1500` as a base parameter
Job **Register Devices - Lakebase Streaming** (`213721155994897`) base parameters:
```
num_devices = 1500
profile     = fevm-target
```
(The job-level parameter is what actually takes effect on a run.)

## 3. All jobs repointed to `tripwise/notebooks`
The standalone `/Workspace/Users/amee.vora@databricks.com/notebooks/` folder contains
**only 8 data notebooks** (`load_sample_data*`, `load_agg_tables`,
`embedded_insurance_v2.sql`, `new_drivers_with_no_trips`, `truncate_devices_lakebase`).
The 5 app/ML notebooks live **only** in `tripwise/notebooks/`. The jobs were pointing at
the standalone paths, which **did not exist** (latent breakage) — repointed to the
tripwise folder.

| Job | ID | Task notebook(s) — new path under `/tripwise/notebooks/` |
|-----|----|----------------------------------------------------------|
| Register Devices - Lakebase Streaming | `213721155994897` | `register_devices_streaming` |
| Device Telemetry Data | `420962304813966` | `ingest_telemetry_zerobus` |
| TripWise Premium Forecast - Train & Deploy | `93417101944426` | `create_feature_table`, `train_premium_model`, `deploy_endpoint` |

Base parameters (e.g. `profile=fevm-target`) were preserved during the repoint.

### Reset pattern used
```bash
databricks jobs get <id> -p fevm-target -o json \
 | jq '{job_id, new_settings: (.settings
        | (.tasks[] | select(.notebook_task) | .notebook_task.notebook_path)
          |= gsub("/Users/amee.vora@databricks.com/notebooks/";
                  "/Users/amee.vora@databricks.com/tripwise/notebooks/"))}' > job.json
databricks jobs reset --json @job.json -p fevm-target
```

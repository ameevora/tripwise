# Execution Evidence — Device Registration → Lakebase

Component: **Lakebase operational serving** (device registration stream).
Notebook: `tripwise/notebooks/register_devices_streaming` (run by job *Register Devices -
Lakebase Streaming*, `213721155994897`).

> The notebook streams registrations with `time.sleep(1)` between batches and does not
> call `dbutils.notebook.exit()`, so the job API returns no stdout text. The execution
> evidence here is the **job run record** plus the **resulting Lakebase table state**,
> whose timestamps correlate exactly with the run window.

## Job run record
| Field | Value |
|-------|-------|
| run_id | `<run-id>` |
| job_id | `213721155994897` |
| result | **SUCCESS** |
| start | `2026-10-22T21:50:17Z` |
| end | `2026-10-22T21:54:15Z` |
| duration | 238 s (~4 min, matches variable-rate streaming) |
| run page | `…/job/213721155994897/run/<run-id>` |

(First task attempt `<run-id>` hit a transient `SIGABRT`; the job's retry
`<run-id>` succeeded.)

## Resulting Lakebase table state — `tripwise_demo.devices`
```
 total_devices | first_id | last_id |    latest_registration
---------------+----------+---------+----------------------------
          1000 | DEV0001  | DEV1000 | 2026-10-22 21:54:12.986199
```
The latest `registered_at` (**21:54:12**) falls inside the run window (21:50–21:54) —
direct proof the run wrote these rows.

### 5 most recently registered
```
 device_id |   driver_name   |       registered_at
-----------+-----------------+----------------------------
 DEV1000   | Isabella Kim    | 2026-10-22 21:54:12.986199
 DEV0999   | Wyatt Fernandez | 2026-10-22 21:54:12.982700
 DEV0998   | Luca Takahashi  | 2026-10-22 21:54:12.978353
 DEV0997   | Theodore Olsen  | 2026-10-22 21:54:12.974356
 DEV0996   | Carter Rossi    | 2026-10-22 21:54:12.970454
```

## How to reproduce
```bash
# run record
databricks jobs get-run <run-id> -p fevm-target -o json
# table state (as instance owner)
HOST=$(databricks postgres list-endpoints projects/tripwise/branches/production -p fevm-target -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential projects/tripwise/branches/production/endpoints/primary -p fevm-target -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p fevm-target -o json | jq -r '.userName')
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" \
  -c "SELECT count(*), min(device_id), max(device_id), max(registered_at) FROM tripwise_demo.devices;"
```

> Data is synthetic (names generated from fixed first/last-name lists in the notebook) —
> no customer data.

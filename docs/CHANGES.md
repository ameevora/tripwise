# TripWise — Change Log (fevm-target)

Index of changes made to the TripWise demo on the **`fevm-target`** workspace
(`https://fevm-serverless-stable-mv5gxq.cloud.databricks.com`). Local working copy:
`/Users/amee.vora/tripwise_fe_bar_latest`.

| Doc | Area |
|-----|------|
| [repo-structure.md](repo-structure.md) | Local repo layout, how it was assembled, dashboard/genie parity |
| [notebooks-and-jobs.md](notebooks-and-jobs.md) | `register_devices_streaming` 1000→1500, job repointing to `tripwise/notebooks` |
| [app-changes.md](app-changes.md) | App code/UI changes (agent + offer tabs, editable email) |
| [lakebase-uc-grants.md](lakebase-uc-grants.md) | Lakebase Postgres + Unity Catalog grants to the app SP |

## Key identifiers
| Thing | Value |
|-------|-------|
| App | `tripwise` — https://tripwise-7474658320355593.aws.databricksapps.com |
| App service principal | `faee39eb-c8e5-4d99-ada7-4deca1cea3e4` |
| App source path | `/Workspace/Users/amee.vora@databricks.com/tripwise` |
| Delta catalog/schema | `serverless_stable_mv5gxq_catalog.insurance_trip_wise` |
| Lakebase project (autoscaling) | `projects/tripwise/branches/production/endpoints/primary` |
| Lakebase online UC catalog | `av_lakebase_tripwise` (`MANAGED_ONLINE_CATALOG`) |
| Warehouse | `22ad910fe1aed92d` |
| Last successful app deployment | `01f1ce7840b518108b126a3f12d1727f` |

## Data sources after these changes
| App feature | Source | Protocol |
|-------------|--------|----------|
| Agent tab — Select Driver list | `tripwise_demo.devices` (name) + `insurance_trip_wise.dim_driver_sync` (score/risk) | Lakebase psycopg2 |
| Agent tab — Driver Details & Policy Adjustment | name ← `av_lakebase_tripwise.tripwise_demo.devices`; behavioral + `policies` ← Delta | SQL warehouse |
| Offer tab — device list | `tripwise_demo.devices` | Lakebase psycopg2 |
| Offer tab — recommendation (incl. driving score) | `tripwise_demo.devices` + `insurance_trip_wise.dim_driver_sync` | Lakebase psycopg2 |

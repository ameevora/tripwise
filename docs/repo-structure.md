# Local repo structure — `tripwise_fe_bar_latest`

How the local working copy was assembled from the `fevm-target` snapshot and the
live workspace.

## Final layout
```
tripwise_fe_bar_latest/
├── README.md
├── docs/                      # this change documentation
└── tripwise/                  # app source (deployed as the `tripwise` app)
    ├── app.py
    ├── app.yaml
    ├── config/                # queries.py, __init__.py
    ├── dashboard_embedding.py
    ├── genie_embedding.py
    ├── requirements.txt
    ├── static/css/styles.css
    ├── templates/             # agent.html, offer.html, analytics*, genie.html, layout.html, registrations.html
    ├── notebooks/             # 5 notebooks the jobs run (see notebooks-and-jobs.md)
    ├── dashboards/            # tripwise_offer_performance.lvdash.json
    └── genie/                 # insurance_trip_and_driver_analytics.json
```

## How it was built
1. Copied everything from `/Users/amee.vora/brickwise/chicago-vibe-coding/fevm-target`
   into `tripwise_fe_bar_latest`, **excluding** backup files (`*bkp*`:
   `app_bkp.py`, `app_bkp.yaml`, `templates/agent_bkp.html`).
2. Re-exported the workspace `tripwise` folder
   (`/Users/amee.vora@databricks.com/tripwise`) over `tripwise/` to pick up the
   nested `notebooks/` and the latest file contents.
3. **Removed** the top-level standalone `notebooks/` folder (the 8 data notebooks:
   `load_sample_data*`, `load_agg_tables`, `embedded_insurance_v2.sql`,
   `new_drivers_with_no_trips`, `truncate_devices_lakebase`) — kept only the app's
   bundled `tripwise/notebooks/`.
4. **Moved** `dashboards/` and `genie/` inside `tripwise/`.

## Dashboard & Genie parity (verified identical to deployed)
- **Dashboard** `tripwise/dashboards/tripwise_offer_performance.lvdash.json`
  - vs deployed `01f15bdebe6a12d8b153f14e54703b8e`: inner `serialized_dashboard`
    **identical** (19 datasets, 4 pages). Metadata matches (warehouse `22ad910fe1aed92d`).
- **Genie** `tripwise/genie/insurance_trip_and_driver_analytics.json`
  - vs deployed space `01f15d6cfd4a14c29378cffba250e64b`: `serialized_space`
    **identical**. (Fetch the deployed serialized form with
    `GET /api/2.0/genie/spaces/{id}?include_serialized_space=true`.)

## Notebook source-of-truth note
The app/ML notebooks live in **`/tripwise/notebooks/`** in the workspace (not the
standalone `/notebooks/` folder). The jobs were repointed accordingly — see
[notebooks-and-jobs.md](notebooks-and-jobs.md).

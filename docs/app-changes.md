# App changes — `tripwise`

All edits made to the Flask app source under `tripwise/`, then synced to
`/Workspace/Users/amee.vora@databricks.com/tripwise` and deployed
(last deployment `01f1ce7840b518108b126a3f12d1727f`, SUCCEEDED).

## 1. Agent tab — Select Driver list now from Lakebase
`app.py` `get_drivers()` (`/api/drivers`) moved **off** the SQL warehouse / Delta
`dim_drivers` and **onto Lakebase** (psycopg2):
- `device_id` + `driver_name` ← `tripwise_demo.devices` (registration table)
- `current_driving_score`, `risk_tier`, `total_trips`, `total_miles`, `vehicle_info`,
  `status` ← `insurance_trip_wise.dim_driver_sync`
- Joined on `device_id`, `WHERE s.status='active'`, `ORDER BY s.current_driving_score DESC`

```sql
SELECT dev.device_id, dev.driver_name,
       s.vehicle_info, s.current_driving_score, s.total_trips,
       s.total_miles, s.risk_tier, s.status
FROM tripwise_demo.devices dev
JOIN insurance_trip_wise.dim_driver_sync s ON dev.device_id = s.device_id
WHERE s.status = 'active'
ORDER BY s.current_driving_score DESC
```
> Inner join ⇒ list is driven by registered devices (1000 now; grows with the
> registration job). Switch to `LEFT JOIN` + `COALESCE(dev.driver_name, s.driver_name)`
> if all `dim_driver_sync` rows should appear regardless of registration.

## 2. Agent tab — Driver Details & Policy Adjustment: name from devices
`app.py` `get_driver_details()` (`/api/driver/<device_id>`) now LEFT JOINs the Lakebase
devices table **via its online UC catalog** and takes the name from there, so the detail
panel and the list show the **same** name. Behavioral fields + `policies` stay on Delta
(via the SQL warehouse).
```sql
SELECT d.device_id, dev.driver_name, d.vehicle_info, d.policy_number,
       d.registration_date, d.current_driving_score, d.total_trips,
       d.total_miles, d.risk_tier, d.status,
       p.policy_id, p.premium_amount, p.premium_period, p.discount_pct,
       p.coverage_level, p.effective_date, p.is_active as policy_is_active
FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers d
LEFT JOIN serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies p
    ON d.device_id = p.device_id AND p.is_active = true
LEFT JOIN av_lakebase_tripwise.tripwise_demo.devices dev
    ON d.device_id = dev.device_id
WHERE d.device_id = '{device_id}' AND p.policy_type != 'embedded_trip'
LIMIT 1
```
Requires UC grants to the app SP — see [lakebase-uc-grants.md](lakebase-uc-grants.md).

## 3. Offer tab — driving score
- `app.py` `get_offer_recommendation()` (`/api/offer_recommendation/<device_id>`):
  `LEFT JOIN insurance_trip_wise.dim_driver_sync s ON d.device_id = s.device_id` to
  return `current_driving_score` (over the existing Lakebase psycopg2 connection). 100%
  of offer-tab devices have a score (verified: 1000/1000 matched).
- `templates/offer.html`: removed the inline `· Score` next to the name; added a dedicated
  **Driving Score** tile (speedometer icon) **between** the name tile and Derived State.
  Row is now four `col-md-3` tiles (name · score · state · trigger). Value is color-coded
  via `getScoreColor()` (≥80 green, ≥60 amber, else red).
- `static/css/styles.css`: added `.stat-icon.orange`.

## 4. Editable generated email (both tabs)
`templates/agent.html` (`#email-preview`) and `templates/offer.html`
(`#offer-email-preview`):
- The preview becomes **`contenteditable="true"` only after an email is generated**
  (locked to `false` on placeholder / error).
- Added an edit hint (`<small>` with a pencil icon) shown alongside the editable email.
- `static/css/styles.css`: `.email-preview[contenteditable="true"]` → `cursor: text` and
  a focus outline.
- **Copy** reads the live element text, so manual edits are preserved on copy.

> **Send** (`sendEmail()` / `sendOfferEmail()`) is still a stub (`alert(...)`, no real
> email integration). When wired up, point it at the same preview element so edits carry
> through.

## Files touched
| File | Change |
|------|--------|
| `tripwise/app.py` | `get_drivers`, `get_driver_details`, `get_offer_recommendation` |
| `tripwise/templates/agent.html` | editable email |
| `tripwise/templates/offer.html` | score tile + editable email |
| `tripwise/static/css/styles.css` | `.stat-icon.orange`, editable email styles |

## Sync & deploy
```bash
# sync each changed file
databricks workspace import /Users/amee.vora@databricks.com/tripwise/<file> \
  --file <file> --format AUTO --overwrite -p fevm-target
# deploy
databricks apps deploy tripwise \
  --source-code-path /Workspace/Users/amee.vora@databricks.com/tripwise -p fevm-target
```

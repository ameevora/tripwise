-- Gold: join only. All data-quality enforcement already happened at the silver hop,
-- so this materialized view is pure enrichment — trip context joined onto telemetry,
-- no expectations needed here.
--
-- Grain: one row per trip (silver_trips), enriched with only the LAST telemetry
-- record for that trip (max timestamp per trip_id) — silver_telemetry has many
-- records per trip, so we pick a single representative record rather than fanning
-- out trip rows across every record.
--
-- Join key: trip_id only. device_id is taken from silver_trips (t.*) — trips and
-- brickwise_device_data were generated independently and do not share a consistent
-- trip_id -> device_id mapping, so joining on device_id as well as trip_id would drop
-- all but a handful of coincidental matches.

CREATE OR REFRESH MATERIALIZED VIEW trip_telemetry_enriched
AS
WITH latest_telemetry AS (
  SELECT
    trip_id,
    event_type,
    is_night_driving,
    phone_usage,
    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY timestamp DESC) AS rn
  FROM silver_telemetry
)
SELECT
  t.*,
  d.event_type,
  d.is_night_driving,
  d.phone_usage
FROM silver_trips AS t
JOIN latest_telemetry AS d
  ON t.trip_id = d.trip_id
 AND d.rn = 1;

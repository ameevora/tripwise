-- Silver: data-quality expectations enforced here, once per source, before anything
-- downstream joins on these fields. Join-key / critical fields DROP the row on
-- violation; range/consistency checks omit ON VIOLATION (the default: tracked and
-- visible in the pipeline UI + lineage, without dropping the row).

CREATE OR REFRESH STREAMING TABLE silver_trips
(
  CONSTRAINT trip_id_not_null EXPECT (trip_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT device_id_not_null EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW,

  CONSTRAINT valid_trip_score EXPECT (trip_score BETWEEN 0 AND 100),
  CONSTRAINT non_negative_distance EXPECT (distance_miles >= 0),
  CONSTRAINT positive_duration EXPECT (duration_seconds > 0),
  CONSTRAINT valid_speed_consistency EXPECT (max_speed_mph >= avg_speed_mph),
  CONSTRAINT valid_night_pct EXPECT (night_driving_pct BETWEEN 0 AND 100),
  CONSTRAINT valid_risk_pct EXPECT (high_risk_area_pct BETWEEN 0 AND 100),
  CONSTRAINT valid_weather_pct EXPECT (inclement_weather_pct BETWEEN 0 AND 100),
  CONSTRAINT offers_ticks_tie EXPECT (offers_clicked <= offers_shown),
  CONSTRAINT chronological_trip EXPECT (start_timestamp <= end_timestamp)
)
AS
SELECT *
FROM STREAM(bronze_trips);

CREATE OR REFRESH STREAMING TABLE silver_telemetry
(
  CONSTRAINT trip_id_not_null EXPECT (trip_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT device_id_not_null EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type EXPECT (
    event_type IN ('normal', 'hard_brake', 'rapid_accel', 'sharp_turn', 'speeding')
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_is_night_driving EXPECT (is_night_driving IS NOT NULL) ON VIOLATION DROP ROW,

  CONSTRAINT valid_phone_usage EXPECT (phone_usage IS NOT NULL)
)
AS
SELECT *
FROM STREAM(bronze_telemetry);

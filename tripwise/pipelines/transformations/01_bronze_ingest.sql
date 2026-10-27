-- Bronze: streaming passthrough ingest of the two existing source tables into the
-- pipeline's own managed tables. No filtering/expectations here by design — bronze
-- is raw, as-landed; expectations are enforced at the silver hop.

CREATE OR REFRESH STREAMING TABLE bronze_trips
AS
SELECT *
FROM STREAM(serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips);

CREATE OR REFRESH STREAMING TABLE bronze_telemetry
AS
SELECT *
FROM STREAM(serverless_stable_mv5gxq_catalog.insurance_trip_wise.brickwise_device_data);

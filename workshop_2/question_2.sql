SELECT
  taxi_zone_pu.Zone as pickup_zone,
  taxi_zone_do.Zone as dropoff_zone,
  avg(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_duration,
  min(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_duration,
  max(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_duration
FROM
  trip_data
JOIN taxi_zone as taxi_zone_pu
  ON trip_data.PULocationID = taxi_zone_pu.location_id
JOIN taxi_zone as taxi_zone_do
  ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY
  taxi_zone_pu.Zone, taxi_zone_do.Zone
ORDER BY
  avg_trip_duration DESC;


CREATE MATERIALIZED VIEW max_avg_trip_duration_time_with_count AS
  WITH t AS (
    SELECT
      avg(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_duration,
      count(*) AS trips_count,
      PULocationID AS pu_location_id,
      DOLocationID AS do_location_id
    FROM
      trip_data
    GROUP BY
      PULocationID, DOLocationID
  ),
  max_avg_t AS (
    SELECT
      max(avg_trip_duration) AS max_avg_trip_duration_time
    FROM
      t
  )

  SELECT
    taxi_zone_pu.Zone as pickup_zone,
    taxi_zone_do.Zone as dropoff_zone,
    t.trips_count
  FROM max_avg_t, t
  JOIN taxi_zone as taxi_zone_pu
    ON t.pu_location_id = taxi_zone_pu.location_id
  JOIN taxi_zone as taxi_zone_do
    ON t.do_location_id = taxi_zone_do.location_id
  WHERE
    t.avg_trip_duration = max_avg_t.max_avg_trip_duration_time;


-- ANSWEAR: 1

CREATE MATERIALIZED VIEW busiest_zones_17_hour AS
  WITH max_pickup_datetime AS (
    SELECT
      max(tpep_pickup_datetime) AS latest_pickup_datetime
    FROM
      trip_data
  )

SELECT
    taxi_zone.Zone AS pickup_zone,
    count(*) AS last_17_hour_pickup_cnt
FROM
    max_pickup_datetime, trip_data
        JOIN taxi_zone
            ON trip_data.PULocationID = taxi_zone.location_id
WHERE
    trip_data.tpep_pickup_datetime > (max_pickup_datetime.latest_pickup_datetime - INTERVAL '17' HOUR)
GROUP BY
    taxi_zone.Zone
ORDER BY last_17_hour_pickup_cnt DESC
    LIMIT 3;


-- Answear: LaGuardia Airport, Lincoln Square East, JFK Airport

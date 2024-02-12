*Question 1: What is count of records for the 2022 Green Taxi Data??

```
SELECT count(*) FROM `de-zoomcamp-407321.trips.green_taxi_trips`
```
840402


* Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Create native table:
```
CREATE TABLE trips.green_taxi_trips_native AS (
  SELECT
    *
  FROM
    `de-zoomcamp-407321.trips.green_taxi_trips`
);
```

Counting:
```
SELECT count(distinct PULocationID) FROM `de-zoomcamp-407321.trips.green_taxi_trips`  -- 258 records, 0 bytes

SELECT count(distinct PULocationID) FROM `de-zoomcamp-407321.trips.green_taxi_trips_native`  -- 258 records, 6.41 Mb
```


* Question 3:
How many records have a fare_amount of 0?

```
SELECT count(*) FROM `de-zoomcamp-407321.trips.green_taxi_trips_native`  where fare_amount = 0 -- 1622 records
```



* Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

```
CREATE TABLE
  trips.green_taxi_trips_part
PARTITION BY
  lpep_pickup_date
CLUSTER BY
  PUlocationID
AS (
  SELECT
    *,
    date(TIMESTAMP_MICROS(cast(lpep_pickup_datetime / 1000 as integer))) lpep_pickup_date
  FROM
    trips.green_taxi_trips_native
);
```


* Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

```
SELECT count(distinct PULocationID) FROM `de-zoomcamp-407321.trips.green_taxi_trips_native`  where date(TIMESTAMP_MICROS(cast(lpep_pickup_datetime / 1000 as integer))) between '2022-06-01' and '2022-06-30' -- 242 records, 12.82 Mb

SELECT count(distinct PULocationID) FROM `de-zoomcamp-407321.trips.green_taxi_trips_part`  where lpep_pickup_date between '2022-06-01' and '2022-06-30' -- 242 records, 1.12 Mb
```


* Question 6:
Where is the data stored in the External Table you created?

GCP Bucket


* Question 7:
It is best practice in Big Query to always cluster your data:

False



* Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```
SELECT count(*) from trips.green_taxi_trips_native -- 0 bytes
```
Maybe because it getch data_rows from table's metadata?

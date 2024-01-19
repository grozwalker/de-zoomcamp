SELECT
	z."Borough",
	sum(yt.total_amount) as amounts
FROM
	yellow_taxi as yt
	left join zones as z on yt."PULocationID" = z."LocationID"
where
	"Borough" != 'Unknown' and date(yt.lpep_pickup_datetime) = '2019-09-18'
group by 1;


SELECT
	trip_distance,
	date(lpep_pickup_datetime) pickup_date
FROM
	yellow_taxi
order by trip_distance desc;

SELECT
	count(*)
FROM
	yellow_taxi
where date(lpep_pickup_datetime) = '2019-09-18' and date(lpep_dropoff_datetime) = '2019-09-18'ж

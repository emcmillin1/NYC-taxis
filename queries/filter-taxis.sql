SELECT
  *
FROM
  `nyc-tlc.yellow.trips`
WHERE
  pickup_datetime < TIMESTAMP(DATE(2015,4,30))
  AND pickup_datetime > TIMESTAMP(DATE(2015,4,15))
  AND passenger_count = 1
  AND vendor_id LIKE "VTS"
  AND EXTRACT(HOUR
  FROM
    pickup_datetime) > 12
  AND EXTRACT(HOUR
  FROM
    pickup_datetime) < 18;

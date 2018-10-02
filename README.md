# Spark-taxis# Spark Taxis Data Manipulation Demo 

## Context:

I found a 130GB public data source containing information about ~1.1B Taxi Rides in New York City. We will be using this data to practice some of the most common data manipulation techniques in Spark (Data Type casting, Filtering, User Defined Functions, Feature Engineering, etc.)

## Data:

To avoid having everybody instantiate and configure a Google Cloud Account I've already performed a query against the Google Big Query dataset in order to get it into a reasonably sized csv for everyone to work with. If anybody is curious about what this looked like: 

```sql
SELECT
  *
FROM
  `nyc-tlc.yellow.trips`
WHERE
  pickup_datetime < TIMESTAMP(DATE(2015,4,30))
  AND pickup_datetime > TIMESTAMP(DATE(2015,4,15))
  AND passenger_count = 1
  AND vendor_id LIKE "VTS"
  AND EXTRACT(HOUR FROM pickup_datetime) > 12
  AND EXTRACT(HOUR FROM pickup_datetime) < 18;
```

## Starter Code:

I already built a Flask app and Leaflet/D3 script to visualize the result of the data filter. This includes a number of input boxes used to pass parameters into the different filter functions that we will be building out.

## Our task:

Everything is working except for the data pipeline at this point. With that said, our task is to build out the spark.py script to do everything we want it to. This includes:

* Reading in the data from csv source

* Casting columns as specific data types

* Defining functions to build out new columns

* Defining functions to filter data 

* Converting The results into JSON format for the D3.json call

* All of these aspects should be parameterized

**I'm probably forgetting a few things, but this should be enough to get started.**
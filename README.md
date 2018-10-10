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

I already built a Flask app and Leaflet/D3 script to visualize the result of the data filter. This includes a number of input boxes used to pass parameters into the different filter functions that we will be building out. **Notice: The script references a file at data/filered_taxis.csv, due to Github's new 100MB file limit this file is zipped. Unzip it before attempting to run the code**

## Our task:

The Flask app and Leaflet Plotting script are already implemented and functioning. Additionally, the base data pipeline, including reading the file from the CSV with a user defined schema, filtering on html input, and returning the matching results in JSON format through the Flask App to the Javascript file. With that said, there are two ways we can approach this problem:

1. ###### We can rebuild the initial data pipeline to work on the initial operations in spark. This would include:

  * Reading in the data from csv source
  * Casting columns as specific data types
  * Defining functions to build out new columns
  * Defining functions to filter data
  * Converting The results into JSON format for the D3.json call
  * All of these aspects should be parameterized

2. ###### We can implement additional filters on top of the existing pipeline in Spark. The available parameters that are already being unpacked in the Flask App include:

  * Trip Distance
  * Trip Distance Filter
  * Payment Type
  * Start Date
  * End

Also, to prepare for next week, I was thinking that we could show how to scale this application to the full dataset by executing the spark job against the Google Big Query Public dataset directly using Google Cloud compute clusters. 

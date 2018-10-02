import subprocess, os
from datetime import timedelta, datetime
from math import sin, cos, sqrt, atan2, radians

import pyspark as ps
from pyspark import SQLContext
from pyspark.sql.functions import udf, to_timestamp, array
from pyspark.sql.types import StructField, StructType, FloatType, StringType, IntegerType, TimestampType

# Import user defined functions 
from udfs import create_haversine_udf, str_to_datetime_udf, multiply_udf

########################################################################
### Data pull
########################################################################

# grab .gz (GBQ file) from cloud storage using subprocess module
# filepath = 'filtered_taxis.gz'
# if subprocess.check_call(f'gsutil -d cp gs://eric-taxis-demo/{filepath} ../data/{filepath}') == 0:
#     # unzip -> location; set csv location
#     if subprocess.check_call(f'gunzip ../data/{filepath}') == 0:
#         csvpath = f"../data/{filepath}"
#     else:
#         raise(f'{filepath} Download Failed')

########################################################################
### spark code
########################################################################

def init(csv_path='data/filtered_taxis.csv'):
    '''
    Initializes primary Spark Dataframe and Creates spark session
    '''
    # initialize spark, df as global variables
    global spark
    global df

    spark = ps.sql.SparkSession.builder \
        .master("local[4]") \
        .appName("Spark Taxis") \
        .getOrCreate()

    schema = StructType([
        
        StructField('vendor_id', StringType(), True),
        StructField('pickup_datetime', StringType(), True),
        StructField('dropoff_datetime', StringType(), True),
        StructField('pickup_longitude', FloatType(), True),
        StructField('pickup_latitude', FloatType(), True),
        StructField('dropoff_longitude', FloatType(), True),
        StructField('dropoff_latitude', FloatType(), True),
        StructField('rate_code', IntegerType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', FloatType(), True),
        StructField('payment_type', StringType(), True),
        StructField('fare_amount', FloatType(), True),
        StructField('extra', FloatType(), True),
        StructField('mta_tax', FloatType(), True),
        StructField('imp_surcharge', FloatType(), True),
        StructField('tip_amount', FloatType(), True),
        StructField('tolls_amount', FloatType(), True),
        StructField('total_amount', FloatType(), True),
        StructField('store_and_fwd_flag', StringType(), True)

    ])
    
    df = spark.read.format("csv").option(
        "header", "true").load(csv_path, schema=schema)


    ########################################################################
    ### Data Cleaning, Feature engineering
    ########################################################################

    # convert str datetimes into datetime type, drop old
    for stage in ['pickup', 'dropoff']:
        df = df \
            .withColumn(f'{stage}_dt', to_timestamp(df[f'{stage}_datetime'])) \
            .drop(f'{stage}_datetime')

    return df


def create_distance_col(df, target_lat, target_lon, units='metric'):
    # gimme global spark session
    global spark
    # global df

    # create custom udf, passing necessary parameters
    haversine_udf = create_haversine_udf(target_lat=target_lat, target_lon=target_lon)

    # create distance column with haversine_udf
    df = df.withColumn('distance', haversine_udf(array('dropoff_latitude', 'dropoff_longitude')))
    return df


def filter_on_distance(df, target_lat, target_lon, distance_threshold, units='meters'):
    # reference global vars
    # global df
    global spark

    df = create_distance_col(df, target_lat, target_lon)
    df = df.filter(df.distance<distance_threshold)

    return df

def filter_on_date(df, dt_str, keep='after'):
    # reference spark session from global scope
    global spark 

    # convert date str to datetype
    # filter either before of after on keep parameter
    pass

def filter_on_payment_type(df, type):
    # reference spark session from global scope
    global spark 

    # filter payment_type column 
    pass


def filter_on_trip_distance(df, distance, how):
    # reference spark session from global scope
    global spark 

    # filter on trip distance
    pass

# initialize dataframe
df = init()



# testing purposes - Only executes when spark.py is run directly
if __name__ == '__main__':
    df = init('data/filtered_taxis.csv')
    filtered_df = filter_on_distance(df, target_lat=40.712508, target_lon=-73.998233, distance_threshold=10000)



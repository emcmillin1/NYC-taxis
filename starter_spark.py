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

    # define Schema 
    # read csv
    # cast dt cols as timestamp types
    

def create_distance_col(df, target_lat, target_lon, units='metric'):
    # gimme global spark session
    global spark
    pass

def filter_on_distance(df, target_lat, target_lon, distance_threshold, units='meters'):
    # reference global vars
    # global df
    global spark
    pass

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
    pass
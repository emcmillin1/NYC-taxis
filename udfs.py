from math import sin, cos, sqrt, atan2, radians
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, array
from pyspark.sql.types import TimestampType, FloatType


# for handling units parameter in haversine function (starts as km)
conversions = {
    'meters': lambda x: x * 1000,
    'miles': lambda x: x * .621371,
    'feet': lambda x: x * 3280.84,
    'kilometer': lambda x: x
}

def haversine(lat, lon, target_lat, target_lon, units='meters'):
   
    if units not in conversions.keys():
        raise("Available units are 'meters', 'miles', 'feet', 'kilometer'")

    # approximate radius of earth in km
    R = 6373.0

    # convert degrees to radians
    lat1 = radians(lat)
    lon1 = radians(lon)
    lat2 = radians(target_lat)
    lon2 = radians(target_lon)

    # distance between lats, longs (in radians)
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d =  R * c
    # convert and return
    return conversions[units](d)

# function to build custom haversine udf to allow us to pass additional params
def create_haversine_udf(target_lat, target_lon, units='meters'):
    return udf(
        lambda arr: 
            haversine(
                arr[0], 
                arr[1], 
                target_lat=target_lat, 
                target_lon=target_lon,
                units=units
            ), 
        FloatType()
    )
    

# Convert improper numeric str to integer
def format_time_part(tp):
    # check if str starts with a 0 (will throw error in datetime construction)
    if tp[0]=='0':
        tp=tp[-1]
    # return integer rep. of str
    return eval(tp)

# function to take string to datetime
def str_to_datetime(dt_str):
    # sample dt_str: '2015-04-21 16:56:08 UTC'
    dt, tm, tz = dt_str.split(' ')
    #unpack date
    yyyy, mm, dd = dt.split('-')

    # unpack time
    hr, mn, sec = tm.split(':')

    # handle month, sec str formatting & cast all as integers
    yyyy, mm, dd, hr, mn, sec = map(format_time_part, [yyyy, mm, dd, hr, mn, sec])

    # build datetime (adjusting hour using timedelta)
    return (datetime(yyyy, mm, dd, hr, mn, sec) - timedelta(0,5)).timestamp()

# register ^ as user defined function to apply to df columns
str_to_datetime_udf =  udf(str_to_datetime, TimestampType())


# practice passing multiple columns into udf
def multiply(a,b):
    return a*b

multiply_udf = udf(lambda arr: multiply(arr[0], arr[1]), FloatType())

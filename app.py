import datetime as dt
import numpy as np
import pandas as pd
import json
from flask import Flask, jsonify, render_template, request
from spark import init as initialize_spark_dataframe, filter_on_distance, filter_on_date, filter_on_payment_type, filter_on_trip_distance, df



#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Home Route
#################################################

@app.route("/")
def form():
    return render_template('index.html')



#################################################################################
### Spark filtering process - Called by d3.json when 
#################################################################################
@app.route("/data")
def data():

    # global df (from spark.py)
    global df

    # Unpack url parameters 
    # implemented filters 
    lat = eval(request.args.get('lat'))
    lon = eval(request.args.get('lon'))
    metric = request.args.get('metric')
    distance = eval(request.args.get('distance'))
    
    # Results truncation
    maxResults = eval(request.args.get('maxResults'))

    # Non-working filters
    tripDistance = request.args.get('tripDistance')
    tripDistanceFilter = request.args.get('tripDistanceFilter')
    paymentType = request.args.get('paymentType')
    start = request.args.get('start')
    end = request.args.get('end')

    # Check the output of read parameters
    print(lat, lon, distance, metric, tripDistance, tripDistanceFilter, paymentType, start, end)
    
    ## Uncomment if global df breaks for whatever reason (slows down execution)
    # df = initialize_spark_dataframe()

    # filter dropoff lat, lon, distance 
    filtered_df = filter_on_distance(df, lat, lon, distance, metric)

    # conditionals to apply/ not apply additional filters

    # date start
    if start!='':
        filtered_df = filter_on_date(filtered_df, start, 'after')

    # date end
    if end!='':
        filtered_df = filter_on_date(filtered_df, end, 'before')

    # payment type
    if paymentType!='':
        filtered_df = filter_on_payment_type(filtered_df, paymentType)

    # trip distance 
    if tripDistance!='':
        filtered_df = filter_on_trip_distance(filtered_df, trip_distance, how=tripDistanceFilter)
        

    # check row 
    print(filtered_df.take(1))


    # return json dump of n<=maxResults records
    return json.dumps(
        filtered_df \
            .toJSON() \
            .map(lambda j: json.loads(j)) \
            .take(maxResults) \
                [0:maxResults]
        )


if __name__ == '__main__':    
    
    
    app.run(debug=True)



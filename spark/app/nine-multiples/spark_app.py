"""
    This Spark app connects to the data source script running in another Docker container on port 9999, which provides a stream of random integers.
    The data stream is read and processed by this spark app, where multiples of 9 are identified, and statistics are sent to a dashboard for visualization.
    Both apps are designed to be run in Docker containers.

    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin
"""


import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession


def aggregate_count(new_values, total_sum):
	  return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def process_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(isMultipleOf9=w[0], count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select isMultipleOf9, count from results order by count")
        new_results_df.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="NineMultiples")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("checkpoint_NineMultiples")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    numbers = data.flatMap(lambda num: [int(num)])
    counts = numbers.map(lambda num: ("Yes" if num % 9 == 0 else "No", 1)).reduceByKey(lambda a, b: a+b)
    windowedCounts = numbers.map(lambda num: ("Yes" if num % 9 == 0 else "No", 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 20, 20)
    windowedCounts.pprint()
    aggregated_counts = counts.updateStateByKey(aggregate_count)
    aggregated_counts.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()

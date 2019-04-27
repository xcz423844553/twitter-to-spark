from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
import requests
import sys

conf = SparkConf()
conf.setAppName("Twitter-To-Spark")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint_Twitter-To-Spark")
dataStream = ssc.socketTextStream("localhost",9090)

def sum_tags_counts(new_values, total_sum):
    return (total_sum or 0) + sum(new_values)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def send_dataframe_to_dashboard(df):
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    url = 'http://0.0.0.0:5050/updateData'
    request_data = {'words': str(top_tags), 'counts': str(tags_count)}
    response = requests.post(url, data=request_data)

def process_rdd(time, rdd):
    print("------------- %s --------------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        print(row_rdd)
        hashtags_df = sql_context.createDataFrame(row_rdd)
        hashtags_df.registerTempTable("hashtag_with_counts")
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtag_with_counts order by hashtag_count desc limit 8")
        hashtag_counts_df.show()
        send_dataframe_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

words = dataStream.flatMap(lambda line: line.split(" "))
#hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
hashtags = words.map(lambda x: (x, 1))
tags_totals = hashtags.updateStateByKey(sum_tags_counts)
tags_totals.foreachRDD(process_rdd)
ssc.start()
ssc.awaitTermination()

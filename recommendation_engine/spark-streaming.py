from __future__ import print_function

import sys
import pyspark_cassandra
import pyspark_cassandra.streaming
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json
import time
# import datetime
from datetime import datetime, timezone, date
from operator import add
"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0,\org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 recommendation_engine/spark-streaming.py localhost:9092 user_event
"""


def getValue(x,key,defVal):
    if key not in x:
        return defVal
    if x[key] is None:
        return defVal
    return x[key]

def getTimeStamp():
    year=datetime.now().year
    month=datetime.now().month
    day=datetime.now().day
    result = int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    return int(datetime.now().timestamp())

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf() \
	.setAppName("spark-streaming") \
	.set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # kvs.pprint()
    parsed = kvs.map(lambda x: json.loads(x[1]))
    # parsed.pprint()
    ob = parsed.map(lambda x: 
        { 
            "idx_user": x['idx_user'],
            "time": x['time'],
            "idx_movie": x['idx_movie'],
            "movie_id": x['movie_id'],
            "value": x['value'],
            "type_event": x['type_event']
            
        })
    ob.pprint()
    ob.saveToCassandra("db","user_event_model")
    ssc.start()
    ssc.awaitTermination()
    pass

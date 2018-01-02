from __future__ import print_function

import sys
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from uuid import uuid1
import json
import time

from datetime import datetime, timezone, date, timedelta



def getGMT():
    year=datetime.now().year
    month=datetime.now().month
    day=datetime.now().day
    result = int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    return result

def getNextGMT():
    result = datetime.now() + timedelta(days=1)
    year = result.year
    month = result.month
    day = result.day
    return int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    pass

"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 recommendation_engine/backup_user_event.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: last_like.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("last-like") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)
    while True:
        current_date = getGMT()
        future_date = getNextGMT()
        rdd = sc.cassandraTable("db","user_event_model").select("idx_user","idx_movie","rating","time","type_event")\
                .filter(lambda x: current_date <= int(x['time']) < future_date)
        if rdd.isEmpty() == False:
            rdd.toDF().write\
                .format('com.databricks.spark.csv')\
                .option("header", "true")\
                .save('/home/trantu/Desktop/engine_recommendation.git/trunk/meta-data/user_event/' \
                        +datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv'))
            # rdd.toDF().write.csv('/home/tutn6/Desktop/engine_recommendation.git/trunk/mycsv')
            # rdd.deleteFromCassandra("db","user_event_model")
        break

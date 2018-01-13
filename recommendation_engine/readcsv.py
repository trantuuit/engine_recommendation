from __future__ import print_function

import sys
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql.types import *
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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 recommendation_engine/readcsv.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: last_like.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("last-like") \
	.set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    df = sql.read.format('com.databricks.spark.csv')\
        .option("header", "true")\
        .load('/home/trantu/Desktop/engine_recommendation.git/trunk/meta-data/20.csv')
    print(df.count())

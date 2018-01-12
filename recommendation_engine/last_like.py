from __future__ import print_function

import sys
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from uuid import uuid1
import json
import time
from pyspark.sql.window import Window
from datetime import datetime, timezone, date, timedelta
from pyspark.sql.functions import rank, col, collect_list, struct


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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 recommendation_engine/last_like.py
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
        # rdd = sc.cassandraTable("db","user_event_model").select("idx_user","time","idx_movie","rating","type_event")\
        #         .filter(lambda x: current_date <= int(x['time']) < future_date)
        #         .filter(lambda x: int(x['rating']) == 5)\
                
                
        # if rdd.isEmpty() == False:
        #     table = rdd.toDF()
        #     result = table.dropDuplicates(['idx_user'])
        #     array = []
        #     for row in result.collect():
        #         x = {
        #             'idx_user': row['idx_user'],
        #             'idx_movie': row['idx_movie'],
        #             'time': row['time']
        #         }
        #         array.append(x)
        #     result = sc.parallelize(array)
        #     result.saveToCassandra('db','last_like_model')

        raw_rdd = sql.read.format("org.apache.spark.sql.cassandra").\
                                load(keyspace="db", table="user_event_model").\
                                filter("time >= %s and time < %s"%(current_date,future_date))
        # print(raw_rdd.collect())   
        rdd1 = raw_rdd.filter("type_event == 'rating' and value = 5").select('idx_user','idx_movie','time')
        rdd2 = raw_rdd.filter("type_event == 'watched' ").select('idx_user','movie_id','time')
        tmp_df1 = rdd1.dropDuplicates(['idx_user'])
        tmp_df1.write.\
            format("org.apache.spark.sql.cassandra").\
            options(table="last_like_model", keyspace="db").\
            save(mode="append")
        
        window = Window.partitionBy(rdd2['idx_user']).orderBy(rdd2['time'].desc())
        tmp_df2 = rdd2.select('*', rank().over(window).alias('rank'))\
        .filter(col('rank') <= 10)\
        .groupBy("idx_user").agg(collect_list(struct(col("movie_id"),col("time"))).alias("recommendations"))
        
        tmp_df2.write.\
            format("org.apache.spark.sql.cassandra").\
            options(table="last_watch_model", keyspace="db").\
            save(mode="append")
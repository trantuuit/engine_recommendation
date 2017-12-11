from __future__ import print_function

import sys
# import pyspark_cassandra
# from pyspark_cassandra import CassandraSparkContext
# from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import rank, col, collect_list, struct
# from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
import csv
from time import time
import logging
import os
from datetime import datetime

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
# sys.path.insert(0, '/home/trantu/Desktop/engine_recommendation.git/trunk/configure/')
# from configureManager import baseConfig
from configure.configureManager import lastActionConfig
config = lastActionConfig()

logging.basicConfig(filename=str(config.path_log_process)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('last-action-process')
log.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

"""
# spark-submit --packages anguenot:pyspark-cassandra:0.6.0 engine_recommendation/last_action_new.py
spark-submit  recommendation_engine/last_action.py meta-data/ratings_20.txt output-data/last-action/last-action.csv
"""

def To_CSV(data, path_output):
    with open(path_output, "a") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(data)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: last-action <input> <output>", file=sys.stderr)
        exit(-1)
    # conf = SparkConf() \
	# .setAppName("last-action") \
	# .set("spark.cassandra.connection.host", "localhost")
    # sc = CassandraSparkContext(conf=conf)
    # spark = SparkSession(sc)
    spark = SparkSession\
        .builder\
        .appName("last-action")\
        .getOrCreate()
    path_input1 = sys.argv[1]
    path_input2 = sys.argv[2]
    # user=sc.cassandraTable("db","user_model").toDF()
    try:
        log.info("+------------------------------------------------------+")
        log.info("+-----------------------BEGIN PROCESS------------------+")
        log.info("+------------------------------------------------------+")
        t0 = time()
        raw_data = spark.read.text(path_input1).rdd
        parts = raw_data.map(lambda row: row.value.split(","))
        events_data = parts.map(lambda x: Row(user_index=int(x[0]),movie_index=int(x[1]), timestamp=int(x[3]))).toDF()
        events_data.show()
        # print(events_data.groupBy('user_index').max('timestamp').collect())
        window = Window.partitionBy(events_data['user_index']).orderBy(events_data['timestamp'].desc())
        result = events_data.select('*', rank().over(window).alias('rank'))\
        .filter(col('rank') <= int(config.top_moive))\
        .groupBy("user_index").agg(collect_list(struct(col("movie_index"),col("timestamp"))).alias("recommendation"))\
        .collect()

        for row in result:
            user_index = row['user_index']
            array=[user_index]
            for r in row['recommendation']:
                array.append(str(r['movie_index']) + "|" + str(r['timestamp']))
            To_CSV( array, path_input2 )
            pass
        log.info("+------------------------------------------------------+")
        log.info("+-----------------------SUCCESSFULLY------------------+")
        log.info("+------------------TAKE TIME: %s---------------------+" %round(time()-t0,3))
        log.info("+------------------------------------------------------+")
        pass
    except:
        log.error("+---------------------------------------------------+")
        log.error("+------------------ERROR-----------------------------+")
        log.error("+---------------------------------------------------+")
        pass

    pass
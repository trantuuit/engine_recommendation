from __future__ import print_function
import sys
import csv
import os
import os.path
from time import time
from pyspark import SparkContext, SparkConf
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from operator import add
from pyspark.sql import Row

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
# sys.path.insert(0, '/home/trantu/Desktop/engine_recommendation.git/trunk/configure/')
# from configureManager import baseConfig
from configure.configureManager import whatIsPopularConfig
config = whatIsPopularConfig()

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

def To_CSV(records, path_output):
    with open(path_output, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(records)
def Train( source_movies, source_ratings, path_output, num):
    log.info("+------------------------------------------------------+")
    log.info("+-----------------------BEGIN PROCESS------------------+")
    log.info("+------------------------------------------------------+")
    t0 = time()

    movies_raw_data = spark.read.format('com.databricks.spark.csv')\
            .option("header", True)\
            .load(source_movies).rdd
    df1 = movies_raw_data.map(lambda x: Row(movie_index=int(x[0]), movie_id=str(x[1]))).toDF()
    # df1.show()

    lines = spark.read.format('com.databricks.spark.csv')\
    .option("header", True)\
    .load(source_ratings)

    # ratings_data_raw = spark.read.text(source_ratings).rdd
    # ratings_data = ratings_data_raw.map(lambda row: row.value.split(","))
    # data = ratings_data.map(lambda x: Row(movie_index=int(x[1]))).toDF()

    ratingsRDD = lines.rdd.map(lambda p: Row( 
                                        movie_index=int(p[1]),
                                        ))
    training = spark.createDataFrame(ratingsRDD)
    df2 = training.groupBy('movie_index').count()
    
    # df2.show()
    result = df2.join(df1, df2.movie_index == df1.movie_index,'inner').sort("count", ascending=False).select('movie_id','count')
    with open(path_output, "a", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for rs in result.take(25):
            # print(rs['movie_id'])
            # print(str(rs['count']))
            writer.writerow([rs['movie_id']] + [str(rs['count'])])

    log.info("+------------------------------------------------------+")
    log.info("+------------------SUCCESSFULLY------------------------+")
    log.info("+------------------TAKE TIME: %s---------------------+" %round(time()-t0,2))
    log.info("+------------------------------------------------------+")
    pass

if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("what is popular")\
    .getOrCreate()
    # sc = SparkContext(appName="what is popular")
    if len(sys.argv) != 4:
        print("Usage: WhatIsPopular <movies_input> <ratings_input> <path_output>", file=sys.stderr)
        exit(-1)
    movies_input = sys.argv[1]
    ratings_input = sys.argv[2]
    path_output = sys.argv[3]
    numtop = int(config.top_moive)
    Train(movies_input, ratings_input, path_output, numtop)
    spark.stop()
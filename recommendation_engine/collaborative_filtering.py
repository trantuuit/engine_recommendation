
from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession


# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row, SQLContext

from time import time
import math
import json
import csv
import os
import logging
from datetime import datetime
import numpy

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from configure.configureManager import collaborativeConfig
config = collaborativeConfig()

logging.basicConfig(filename=str(config.path_log_process)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('collaborative-filtering-process')
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


def To_CSV(data, path_output):
    with open(path_output, "a") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(data)

def train(path_input, path_output):
    log.info("+------------------------------------------------------+")
    log.info("+----------------------BEGIN GET DATA INTO RDD--------+")
    log.info("+------------------------------------------------------+")
    
    lines = spark.read.format('com.databricks.spark.csv')\
    .option("header", True)\
    .load(path_input)

    # print(lines.count())
    # lines.show()
    # lines = spark.read.text(path_input).rdd
    # parts = lines.map(lambda row: row.value.split(","))

    # lines = spark.read.text(path_input).rdd

    # parts = lines.map(lambda row: row.value.split(","))

    # ratingsRDD = parts.map(lambda p: Row(idx_user=int(p[0]),idx_movie=int(p[1]),rating=float(p[2])))
    # print(ratingsRDD.take(1))
    ratingsRDD = lines.rdd.map(lambda p: Row(idx_user=int(p[1]),idx_movie=int(p[0]),rating=float(p[2])))
    print(ratingsRDD.count())
    training = spark.createDataFrame(ratingsRDD)
    log.info("+------------------------------------------------------+")
    log.info("+------------------ENG GET DATA INTO RDD--------------+")
    log.info("+------------------------------------------------------+")
    _train(training, path_output)
    


    pass

def _train(source_training, path_output):
    # print(source_training.take(1))
    log.info("+------------------------------------------------------+")
    log.info("+-----------------BEGIN TRAIN MODEL--------------------+")
    log.info("+------------------------------------------------------+")
    als = ALS(userCol="idx_user", itemCol="idx_movie", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(source_training)
    userRecs = model.recommendForAllUsers(30)
    log.info("+------------------------------------------------------+")
    log.info("+------------------END TRAIN MODEL---------------------+")
    log.info("+------------------------------------------------------+")
    print(userRecs.count())
    # userRecs.show()
    # print("type:%s" %type(userRecs))
    # print(type(userRecs.toJSON()))
    # print(userRecs.toJSON(False).collect())
    # pandas_df = userRecs.toPandas()
    # pandas_df.to_json("/home/tutn6/Desktop/sandbox_recommendation/save/text7.json")
    log.info("+------------------------------------------------------+")
    log.info("+----------------BEGIN EXPORT DATA INTO FILE CSV-------+")
    log.info("+------------------------------------------------------+")
    for i in userRecs.collect():
        result = [i['idx_user']]
        recommendations=i['recommendations']
        for j in recommendations:
            index_movie = str(j['idx_movie'])
            rating = str(j['rating'])
            result.append(index_movie+'|'+rating)
        To_CSV(result,path_output)
    log.info("+------------------------------------------------------+")
    log.info("+----------------END EXPORT DATA INTO FILE CSV--------+")
    log.info("+------------------------------------------------------+")
    # list_userRects = map(lambda row: row.asDict(), userRecs.collect())

    # print(list_userRects.toJSON())
    # print(list_userRects)
    # for i in list_userRects:
    #     print(i)
    #     break
    # with open(path_output, 'w') as outfile:
    #     json.dump(list_userRects, outfile)

    pass

"""
spark-submit recommendation_engine/collaborative_filtering.py meta-data/20.csv/ 
output-data/collaborative-filtering/result.csv
"""
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: collaborative_filtering <path_input> <path_output>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Collaborative_Filtering")\
        .getOrCreate()
    sqlContext = SQLContext(spark)
    t0 = time()
    log.info("+--------------------------------------------------------------+")
    log.info("+--------------------------Read path---------------------------+")
    log.info("+--------------------------------------------------------------+")
    path_input = sys.argv[1]
    path_output = sys.argv[2]

    train(path_input, path_output)
    log.info("+------------------------------------------------------+")
    log.info("+----------------------SUCCESFFULY---------------------+")
    log.info("+------------------------------------------------------+")
    log.info("+------------------------------------------------------+")
    log.info("+--------------total time: %s----------------------+" %round(time()-t0,2))
    log.info("+------------------------------------------------------+")
    spark.stop()

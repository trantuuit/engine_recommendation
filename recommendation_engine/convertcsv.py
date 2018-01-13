from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession


# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from time import time
import math
import json
import csv
import os
import logging
from datetime import datetime
import numpy

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: collaborative_filtering <path_input> <path_output>", file=sys.stderr)
        exit(-1)

    path_input = sys.argv[1]
    path_output = sys.argv[2]
    spark = SparkSession\
        .builder\
        .appName("Collaborative_Filtering")\
        .getOrCreate()
    
    lines = spark.read.text(path_input).rdd
    parts = lines.map(lambda row: row.value.split(","))
    ratingsRDD = parts.map(lambda p: Row(
                                         idx_user=int(p[0]), 
                                         idx_movie=int(p[1]),
                                         time=long(p[3]),
                                         rating=float(p[2]),
                                         type_event='rating')
                            )
    training = spark.createDataFrame(ratingsRDD)
    # training.take(4000000)
    (training,test)= training.randomSplit([1.0, 0.0])
    training.write\
        .format('com.databricks.spark.csv')\
        .option("header", "true")\
        .save(path_output+'20_user_event.csv')
        # .save(path_output+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv'))
    # ratingsRDD.toDF().write\
    #     .format('com.databricks.spark.csv')\
    #     .option("header", "true")\
    #     .save(path_output+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv'))
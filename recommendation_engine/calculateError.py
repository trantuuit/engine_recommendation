from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession

# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: collaborative_filtering <path_input>", file=sys.stderr)
        exit(-1)
    path_input = sys.argv[1]
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .getOrCreate()

    # $example on$
    # lines = spark.read.text("data/mllib/als/sample_movielens_ratings.txt").rdd
    # parts = lines.map(lambda row: row.value.split("::"))
    # ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
    #                                      rating=float(p[2]), timestamp=long(p[3])))

    lines = spark.read.format('com.databricks.spark.csv')\
    .option("header", True)\
    .load(path_input)
    ratingsRDD = lines.rdd.map(lambda p: Row(idx_user=int(p[0]), 
                                        idx_movie=int(p[1]),
                                        rating=float(p[2])))

    ratings = spark.createDataFrame(ratingsRDD)
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    als = ALS(rank=12, maxIter=15, userCol= "idx_user", itemCol= "idx_movie", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions_train = model.transform(training)
    evaluator_train = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse_train = evaluator_train.evaluate(predictions_train)
    print("Root-mean-square error-train = " + str(rmse_train))


    predictions_test = model.transform(test)
    evaluator_test = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse_test = evaluator_test.evaluate(predictions_test)
    print("Root-mean-square error-test = " + str(rmse_test))

    # Generate top 10 movie recommendations for each user
    # userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    # movieRecs = model.recommendForAllItems(10)
    # $example off$
    # userRecs.show()
    # movieRecs.show()

    spark.stop()
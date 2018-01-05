from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession

# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import round
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: check_movie_len <path_input>", file=sys.stderr)
        exit(-1)

    path_input = sys.argv[1]
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .getOrCreate()

    lines = spark.read.text(path_input).rdd
    parts = lines.map(lambda row: row.value.split("::"))
    ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                         rating=float(p[2]), timestamp=long(p[3])))
    ratings = spark.createDataFrame(ratingsRDD)
    (training, validate, test) = ratings.randomSplit([0.7, 0.2, 0.1])
    als = ALS(userCol= "userId", itemCol= "movieId", ratingCol="rating",
              coldStartStrategy="drop")

    model = als.fit(training)


    # predictions = model.transform(training).select('userId','movieId','rating',round('prediction',2).alias('prediction'))
    predictions = model.transform(training)
    # predictions.show()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")                     
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    # predictions = model.transform(validate).select('userId','movieId','rating',round('prediction',2).alias('prediction'))
    predictions = model.transform(validate)
    # predictions.show()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")                     
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))


    # predictions = model.transform(test).select('userId','movieId','rating',round('prediction',2).alias('prediction'))
    predictions = model.transform(test)
    # predictions.show()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")                     
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))
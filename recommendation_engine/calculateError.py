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
    (training, validate, test) = ratings.randomSplit([0.8, 0.15, 0.05])
    print('training: %s' %(training.count()))
    (training_2, training_4, training_6, remain) = training.randomSplit([0.125, 0.25, 0.375, 0.25])
    (training_8, remain ) = training.randomSplit([0.5, 0.5])
    (training_10, remain ) = training.randomSplit([0.625, 0.375])
    (training_12, remain ) = training.randomSplit([0.75, 0.25])
    (training_14, remain ) = training.randomSplit([0.875, 0.125])
    training_16 = training
    print('total 2: %s' %(training_2.count()))
    print('total 4: %s' %(training_4.count()))
    print('total 6: %s' %(training_6.count()))
    print('total 8: %s' %(training_8.count()))
    print('total 10: %s' %(training_10.count()))
    print('total 12: %s' %(training_12.count()))
    print('total 14: %s' %(training_14.count()))
    print('total 16: %s' %(training_16.count()))

    als = ALS(userCol= "idx_user", itemCol= "idx_movie", ratingCol="rating",
              coldStartStrategy="drop")
    #------------------------------------------------
    #-------------------model 4 million -------------
    #------------------------------------------------

    print('+--------------------------------------------------+')
    print('+------4 million training dataset-----------------+')
    print('+--------------------------------------------------+')


    model = als.fit(training_4)

    predictions = model.transform(training_4)
    #-----------RMSE---------------------------
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
                                    
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))

    #-------------MAE--------------------
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    #------------------------------------------------
    #------------------model 6 million --------------
    #------------------------------------------------
    print('+--------------------------------------------------+')
    print('+------6 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training_6)

    predictions = model.transform(training_6)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))

    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))

    #-----------------MAE--------------------
    predictions = model.transform(training_6)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    #----------------model 8 million----------------------
    print('+--------------------------------------------------+')
    print('+------8 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training_8)

    predictions = model.transform(training_8)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))

    #-----------------MAE--------------------
    predictions = model.transform(training_8)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    #------------------------------------------------------
    #----------------model 10 million----------------------
    #------------------------------------------------------
    print('+--------------------------------------------------+')
    print('+------10 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training_10)

    predictions = model.transform(training_10)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))
    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))


    #----------------------------------------
    #-----------------MAE--------------------
    #----------------------------------------
    
    predictions = model.transform(training_10)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    #------------------------------------------------------
    #----------------model 12 million----------------------
    #------------------------------------------------------
    print('+--------------------------------------------------+')
    print('+------12 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training_12)

    predictions = model.transform(training_12)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))
    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))


    #----------------------------------------
    #-----------------MAE--------------------
    #----------------------------------------

    predictions = model.transform(training_12)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    #----------------model 14 million----------------------
    print('+--------------------------------------------------+')
    print('+------14 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training_14)

    predictions = model.transform(training_14)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))

    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))


    #----------------------------------------
    #-----------------MAE--------------------
    #----------------------------------------

    predictions = model.transform(training_14)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))
    

    #----------------model 16 million----------------------
    print('+--------------------------------------------------+')
    print('+------16 million training dataset-----------------+')
    print('+--------------------------------------------------+')

    model = als.fit(training)
    
    predictions = model.transform(training)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-training = " + str(rmse))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-validate = " + str(rmse))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error-test = " + str(rmse))

    #----------------------------------------
    #-----------------MAE--------------------
    #----------------------------------------
    predictions = model.transform(training)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-training = " + str(mae))

    predictions = model.transform(validate)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-validate = " + str(mae))

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating",
                                    predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    print("Mean-absolute error-test = " + str(mae))

    spark.stop()
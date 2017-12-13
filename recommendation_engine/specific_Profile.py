from __future__ import division
from pyspark import SparkContext
from time import time
import os
import os.path
import csv
from pyspark.sql import SparkSession
import sys



def map_percent(x):
    res = ''
    for item in x[1]:
        res += item[0]+':' + str(item[1]) + '|'
    res = res[:-1]
    return (x[0], res)


def getValue(number):
    if number == None:
        return 0
    else:
        return number
    
if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("what is popular")\
    .getOrCreate()
    if len(sys.argv) != 3:
        print("Usage: specific_Profile <movies_input> <path_output>")
        exit(-1)
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    dir = input2
    if not os.path.exists(dir):
        os.mkdir(dir)

    # Get movie data to RDD
    t0 = time()
    movies_raw_data = spark.read.format('com.databricks.spark.csv')\
            .option("header", True)\
            .load(input1).rdd
    # x=6 actor, x=5 writter, x=3 genres, x=4 director
    #---------------------------------actor------------------------------------------
    filename = "ActorReulst"
    movies_data = movies_raw_data.map(lambda token: (token[0], token[1], token[2], token[3], token[4], token[5], token[6], (token[12])))\
                                .filter(lambda x: x[6] is not None)\
                                .map(lambda x: (x[6],x[1],getValue(x[7])))
    
    # #---------------------------------------------------------------------------
    flat_events = movies_data.flatMap(lambda x: [(w, (x[1], float(x[2]))) for w in x[0].split('|')])\
                                .filter(lambda x: x[0] !='')
    event_join = flat_events.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:25] ))\
                            .map(lambda x: map_percent(x))
    print('After group:' + str(event_join.take(5)))
    specs = event_join.collect()
    # # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in specs:
            spamwriter.writerow([item[0]] + [item[1]])
    #---------------------------------------------------------------------------------


    #---------------------------------writer------------------------------------------
    filename = "WriterReulst"
    movies_data = movies_raw_data.map(lambda token: (token[0], token[1], token[2], token[3], token[4], token[5], token[6], (token[12])))\
                                .filter(lambda x: x[5] is not None)\
                                .map(lambda x: (x[5],x[1],getValue(x[7])))
    
    # #---------------------------------------------------------------------------
    flat_events = movies_data.flatMap(lambda x: [(w, (x[1], float(x[2]))) for w in x[0].split('|')])\
                                .filter(lambda x: x[0] !='')
    event_join = flat_events.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:25] ))\
                            .map(lambda x: map_percent(x))
    print('After group:' + str(event_join.take(5)))
    specs = event_join.collect()
    # # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in specs:
            spamwriter.writerow([item[0]] + [item[1]])
    #---------------------------------------------------------------------------------

    #---------------------------------Genres------------------------------------------
    filename = "GenresReulst"
    movies_data = movies_raw_data.map(lambda token: (token[0], token[1], token[2], token[3], token[4], token[5], token[6], (token[12])))\
                                .filter(lambda x: x[3] is not None)\
                                .map(lambda x: (x[3],x[1],getValue(x[7])))
    
    # #---------------------------------------------------------------------------
    flat_events = movies_data.flatMap(lambda x: [(w, (x[1], float(x[2]))) for w in x[0].split('|')])\
                                .filter(lambda x: x[0] !='')
    event_join = flat_events.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:25] ))\
                            .map(lambda x: map_percent(x))
    print('After group:' + str(event_join.take(5)))
    specs = event_join.collect()
    # # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in specs:
            spamwriter.writerow([item[0]] + [item[1]])
    #---------------------------------------------------------------------------------

    #---------------------------------Director------------------------------------------
    filename = "DirectorReulst"
    movies_data = movies_raw_data.map(lambda token: (token[0], token[1], token[2], token[3], token[4], token[5], token[6], (token[12])))\
                                .filter(lambda x: x[4] is not None)\
                                .map(lambda x: (x[4],x[1],getValue(x[7])))
    
    # #---------------------------------------------------------------------------
    flat_events = movies_data.flatMap(lambda x: [(w, (x[1], float(x[2]))) for w in x[0].split('|')])\
                                .filter(lambda x: x[0] !='')
    event_join = flat_events.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:25] ))\
                            .map(lambda x: map_percent(x))
    print('After group:' + str(event_join.take(5)))
    specs = event_join.collect()
    # # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in specs:
            spamwriter.writerow([item[0]] + [item[1]])
    #---------------------------------------------------------------------------------
    tt = time() - t0
    print("------------Completed! It takes %s--------------" % round(tt, 3))
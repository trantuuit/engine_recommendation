from __future__ import division
from pyspark import SparkContext
from time import time
import os
import os.path
import csv
from pyspark.sql import SparkSession

genretokenize = lambda doc: doc.lower().split("|")

def split_csv(line):
    if '\"' in line:
        result = []
        parts = line.split("\"")
        leng = len(parts)
        for j in range(0, leng):
            if(parts[j] != ''):
                if(j % 2 == 0):
                    res = parts[j].split(",")
                    for r in res:
                        if r != '':
                            result.append(r)
                else:
                    result.append(parts[j])
        return result
    else:
        return line.split(",")

def map_percent(x):
    res = ''
    for item in x[1]:
        res += item[0]+':' + str(item[1]) + '|'
    res = res[:-1]
    return (x[0], res)

def write(item, writer):
    writer.writerow([item[0]] + [item[1]])
def getValue(number):
    if number == None:
        return 0
    else:
        return number
    
if __name__ == "__main__":
    dir = "/home/tutn6/Desktop/engine_recommendation.git/trunk/output-data/user-profile"
    filename = "Actor_All_Sorted3"
    if not os.path.exists(dir):
        os.mkdir(dir)
    spark = SparkSession\
    .builder\
    .appName("what is popular")\
    .getOrCreate()
    # Get movie data to RDD
    t0 = time()
    movies_raw_data = spark.read.format('com.databricks.spark.csv')\
            .option("header", True)\
            .load("/home/tutn6/Desktop/engine_recommendation.git/trunk/meta-data/movies.csv").rdd
    # print(movies_raw_data.toDF().first())
    # movies_raw_data = sc.textFile("/home/tutn6/Desktop/engine_recommendation.git/trunk/meta-data/movies.csv")
    # ratings_data = movies_raw_data.map(lambda row: row.value.split(","))
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

    tt = time() - t0
    print("------------Completed! It takes %s--------------" % round(tt, 3))
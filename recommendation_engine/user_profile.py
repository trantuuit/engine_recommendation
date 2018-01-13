from __future__ import division
from pyspark import SparkContext
from time import time
import os
import os.path
import csv
import sys
from pyspark.sql import SparkSession
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

def getGenre(movies_data, events_data):
    # GENRE PERCENT
    filename = "GenresProfileResult"
    movies_data = movies_data.filter(lambda x: x[1][2].strip() != '')
    resj = events_data.join(movies_data)
    res = resj.map(lambda x: (x[1][0],x[1][1][2]))

    print('After map:' + str(res.take(2)))
    flat_events = res.flatMap(lambda x: [(x[0], w) for w in x[1].split('|')]).filter(lambda x: x[1] !='')
    flatevents_zip = flat_events.map(lambda x: ((x[0],x[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)\
                                
    events_count = flatevents_zip.map(lambda x: (x[0][0], (x[0][1], x[1]))) #Userid, Genre, number

    events_all_per_user = flatevents_zip.map(lambda x: (x[0][0], x[1]))\
                                .reduceByKey(lambda x, y: x + y)            #Userid, allnumber
    event_join1 = events_all_per_user.join(events_count).map(lambda x: (x[0],(x[1][1][0],round(100*int(x[1][1][1])/int(x[1][0]),3))))
                                                      
    event_join = event_join1.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:10] ))\
        .map(lambda x: map_percent(x)) #('1302', (224, ('Short', 10)))

    profiles = event_join.collect()
    # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in profiles:
            spamwriter.writerow([item[0]] + [item[1]])
    pass

def getDirector(movies_data, events_data):
    # DERECTOR PERCENT
    filename = "DirectorProfileResult"
    movies_data = movies_data.filter(lambda x: x[1][3].strip() != '')
    res = events_data.join(movies_data).map(lambda x: (x[1][0], x[1][1][3]))

    print('After map:' + str(res.take(2)))
    flat_events = res.flatMap(lambda x: [(x[0], w) for w in x[1].split('|')]).filter(lambda x: x[1] !='')
    flatevents_zip = flat_events.map(lambda x: ((x[0],x[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)\
                                
    events_count = flatevents_zip.map(lambda x: (x[0][0], (x[0][1], x[1]))) #Userid, Genre, number

    events_all_per_user = flatevents_zip.map(lambda x: (x[0][0], x[1]))\
                                .reduceByKey(lambda x, y: x + y)            #Userid, allnumber
    event_join1 = events_all_per_user.join(events_count).map(lambda x: (x[0],(x[1][1][0],round(100*int(x[1][1][1])/int(x[1][0]),3))))
                                                      
    event_join = event_join1.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:10] ))\
        .map(lambda x: map_percent(x)) #('1302', (224, ('Short', 10)))

    profiles = event_join.collect()
    # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in profiles:
            spamwriter.writerow([item[0]] + [item[1]])
    pass
    
def getWriter(movies_data, events_data):
    # WRITER PERCENT
    filename = "WriterProfileResult"
    movies_data = movies_data.filter(lambda x: x[1][4].strip() !='')
    res = events_data.join(movies_data).map(lambda x: (x[1][0], x[1][1][4]))

    flat_events = res.flatMap(lambda x: [(x[0], w) for w in x[1].split('|')]).filter(lambda x: x[1] !='')
    flatevents_zip = flat_events.map(lambda x: ((x[0],x[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)
                                
    events_count = flatevents_zip.map(lambda x: (x[0][0], (x[0][1], x[1]))) #Userid, Genre, number

    events_all_per_user = flatevents_zip.map(lambda x: (x[0][0], x[1]))\
                                .reduceByKey(lambda x, y: x + y)            #Userid, allnumber
    event_join1 = events_all_per_user.join(events_count).map(lambda x: (x[0],(x[1][1][0],round(100*int(x[1][1][1])/int(x[1][0]),3))))
                                                      
    event_join = event_join1.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:10] ))\
        .map(lambda x: map_percent(x)) #('1302', (224, ('Short', 10)))

    profiles = event_join.collect()
    # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in profiles:
            spamwriter.writerow([item[0]] + [item[1]])
    pass

def getActor(movies_data, events_data):
    # ACTOR PERCENT
    filename = "ActorProfileResult"
    movies_data = movies_data.filter(lambda x: x[1][5].strip() != '')
    res = events_data.join(movies_data).map(lambda x: (x[1][0],x[1][1][5]))

    print('After map:' + str(res.take(2)))
    flat_events = res.flatMap(lambda x: [(x[0], w) for w in x[1].split('|')]).filter(lambda x: x[1] !='')
    flatevents_zip = flat_events.map(lambda x: ((x[0],x[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)\
                                
    events_count = flatevents_zip.map(lambda x: (x[0][0], (x[0][1], x[1]))) #Userid, Genre, number

    events_all_per_user = flatevents_zip.map(lambda x: (x[0][0], x[1]))\
                                .reduceByKey(lambda x, y: x + y)            #Userid, allnumber
    event_join1 = events_all_per_user.join(events_count).map(lambda x: (x[0],(x[1][1][0],round(100*int(x[1][1][1])/int(x[1][0]),3))))
                                                      
    event_join = event_join1.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:10] ))\
        .map(lambda x: map_percent(x)) #('1302', (224, ('Short', 10)))

    profiles = event_join.collect()
    # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in profiles:
            spamwriter.writerow([item[0]] + [item[1]])
    pass
if __name__ == "__main__":
    sc = SparkContext(appName="user profile")
    spark = SparkSession(sc)
    # spark = SparkSession\
    # .builder\
    # .appName("Collaborative_Filtering")\
    # .getOrCreate()
    if len(sys.argv) != 4:
        print("Usage: user_profile <input_movies> <input_ratings_20> <output>")
        """
        spark-submit recommendation_engine/user_profile.py meta-data/movies.csv meta-data/ratings_20.txt  output-data/user-profile/
        """
        exit(-1)
    
    path_input1 = sys.argv[1]
    path_input2 = sys.argv[2]
    path_input3 = sys.argv[3]
    dir = path_input3
    if not os.path.exists(dir):
        os.mkdir(dir)
    
    
    # movies_raw_data = sc.textFile(path_input1)
    movies_raw_data =sc.textFile(path_input1).map(lambda line: tuple(list(csv.reader([line]))[0]))
    movies_data = movies_raw_data.map(lambda token: (token[0], (token[1], token[2], token[3], token[4], token[5], token[6])))
    # tt = time() - t0
    
    # events_raw_data = sc.textFile(path_input2)
    lines = spark.read.format('com.databricks.spark.csv')\
        .option("header", True)\
        .load(path_input2)
    # lines.show()
        # ratingsRDD = lines.rdd.map(lambda p: Row( 
        #                                 movie_index=int(p[1]),
        #                                 ))
    # events_data = events_raw_data.map(lambda line: line.split(",")).map(lambda x: (x[1], x[0]))
    
    events_data = lines.rdd.map(lambda x: (x[0], x[1]))
    
    
    # getDirector(movies_data,events_data)
    t0 = time()
    # getDirector(movies_data,events_data)
    # getGenre(movies_data,events_data)
    # getWriter(movies_data,events_data)
    getActor(movies_data,events_data)
    print("Completed collect! It take %s" % round(time()-t0, 2))
    
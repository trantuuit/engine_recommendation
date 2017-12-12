from __future__ import division
from pyspark import SparkContext
from time import time
import os
import os.path
import csv

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


    
if __name__ == "__main__":
    dir = "/home/tutn6/Desktop/engine_recommendation.git/trunk/output-data/user-profile"
    filename = "Movie_Genre_Profile_20000"
    if not os.path.exists(dir):
        os.mkdir(dir)
    sc = SparkContext(appName="user profile")
    # Get movie data to RDD
    t0 = time()

    #IMDB
    movies_raw_data = sc.textFile("/home/tutn6/Desktop/engine_recommendation.git/trunk/meta-data/movies.csv")
    movies_data = movies_raw_data.map(lambda line: split_csv(line)).map(lambda token: (token[0], (token[1], token[2], token[3], token[4], token[5], token[6])))
    # print(movies_data.take(5))

    # # MOVIELEN
    # movies_raw_data = sc.textFile("D:\HOCTAP\HocKy7\Recommendation\MOVIELENS DATA\ml-latest\movies.csv")
    # movies_data = movies_raw_data.map(lambda line: split_csv(line)).map(lambda token: ((token[1], token[2])))
    # movieslen_zip = movies_data.zipWithIndex().map(lambda x: (str(x[1]), x[0]))
    # # print(movieslen_zip.take(5))

    tt = time() - t0
    print("Completed collect! It take %s" % round(tt, 3))
    events_raw_data = sc.textFile("/home/tutn6/Desktop/engine_recommendation.git/trunk/meta-data/ratings_20.txt")
    events_data = events_raw_data.map(lambda line: line.split(",")).map(lambda x: (x[1], x[0]))
    # print(events_data.take(5))

    # # MOVIELEN GENRE PERCENT
    # res = events_data.join(movieslen_zip)\
    #     .map(lambda x: (x[1][0],x[1][1][1]))
    # # print(res.take(5))


    # GENRE PERCENT
    # movies_data = movies_data.filter(lambda x: x[1][2].strip() != '')
    # resj = events_data.join(movies_data)
    # res = resj.map(lambda x: (x[1][0],x[1][1][2]))
    # print('After map:' + str(res.take(2)))

    # DERECTOR PERCENT
    # movies_data = movies_data.filter(lambda x: x[1][3].strip() != '')
    # filename = "Movie_Director_Profile_20000_2"
    # res = events_data.join(movies_data).map(lambda x: (x[1][0], x[1][1][3]))

    # WRITER PERCENT
    # movies_data = movies_data.filter(lambda x: x[1][4].strip() != '')
    # filename = "Movie_Writer_Profile_20000"
    # res = events_data.join(movies_data).map(lambda x: (x[1][0], x[1][1][4]))


    # ACTOR PERCENT
    movies_data = movies_data.filter(lambda x: x[1][5].strip() != '')
    filename = "Movie_Actor_Profile_20000"
    res = events_data.join(movies_data).map(lambda x: (x[1][0],x[1][1][5]))

    #---------------------------------------------------------------------------
    flat_events = res.flatMap(lambda x: [(x[0], w) for w in x[1].split('|')])
    # print("after flat map: "+str(flat_events.take(5)))
    flatevents_zip = flat_events.map(lambda x: ((x[0],x[1]), 1))\
                                .reduceByKey(lambda x, y: x + y)\
                                
    # print(flatevents_zip.count());
    print("after reduce: "+str(flatevents_zip.take(25))) #(('129', 'Short'), 38)
    events_count = flatevents_zip.map(lambda x: (x[0][0], (x[0][1], x[1]))) #Userid, Genre, number
    print(" after extract: "+str(events_count.take(5)))
    events_all_per_user = flatevents_zip.map(lambda x: (x[0][0], x[1]))\
                                .reduceByKey(lambda x, y: x + y)            #Userid, allnumber
    print("Plus all: "+ str(events_all_per_user.take(5)))
    event_join1 = events_all_per_user.join(events_count).map(lambda x: (x[0],(x[1][1][0],round(100*int(x[1][1][1])/int(x[1][0])))))
                                    
    print('list got: %s' %str(event_join1.take(5)))                      
    event_join = event_join1.groupByKey().map(lambda x : (x[0], sorted(list(x[1]), key=lambda x: x[1],reverse=True)[:10] ))\
        .map(lambda x: map_percent(x)) #('1302', (224, ('Short', 10)))
    print("Percent: "+ str(event_join.take(10)))
    # print("num after join: "+str(event_join.count()))
    profiles = event_join.collect()
    # Write to CSV file
    with open(os.path.join(dir, filename + '.csv'), "a", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for item in profiles:
            spamwriter.writerow([item[0]] + [item[1]])
        # event_join.map(lambda x: write(x, spamwriter))
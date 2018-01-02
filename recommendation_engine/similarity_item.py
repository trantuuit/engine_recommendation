from __future__ import print_function
import pandas as pd
import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import os
import os.path
import csv
import numpy as np
import sys
# from pyspark.sql import SparkSession
from datetime import datetime
import logging

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from configure.configureManager import similarityConfig
config = similarityConfig()

logging.basicConfig(filename=str(config.path_log_process)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('similarity-item-process')
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

def _train(path_input, path_output, numrow, numtop):

    tf = TfidfVectorizer(analyzer='word', ngram_range=(1, 3), min_df=1, stop_words='english', encoding='utf-8')
    x = path_input['title'] + ' ' + path_input['genres'].str.replace('|', ' ')  + ' ' + path_input['directors'].str.replace('|', ' ') + ' ' + path_input['writers'].str.replace('|', ' ')
    
    tfidf_matrix = tf.fit_transform(x.values.astype('U'))
    
    index = 0
    totalRow = len(path_input.index)
    print(totalRow)
    if int(totalRow) < int(numrow):
        cosine_similarities = linear_kernel(tfidf_matrix,tfidf_matrix)
        i = 0
        for idx in range(0, int(totalRow)):
            similar_indices = cosine_similarities[i].argsort()[:-int(totalRow):-1]
            similar_items = [str(path_input['movieId'][idx])]
            for j in similar_indices:
                if(idx != j):
                    similar_items.append(str(path_input['movieId'][j])+"|"+str(cosine_similarities[i][j]))
            To_CSV(similar_items,path_output)
            i = i+1 
        pass
    else:
        count = int(int(totalRow)/int(numrow))
        print('--count: %s' %count)
        remain = int(totalRow) - int(count) * int(numrow)
        while( index < count + 1):
            print('--index: %s' %index)
            begin = index * int(numrow)
            # print('---begin: %s' %(begin))
            if ( index == count ):
                if int(remain) == 0:
                    end = begin + int(numrow)
                    # print('---end:%s' %(end))
                else:
                    print('--remain: %s' %(remain))
                    end = begin + int(remain)
            else:     
                end = begin + int(numrow)
                # print('---end:%s' %(end))

            # print(tfidf_matrix[begin:end])
            print('----begin: %s, end: %s' %(begin,end))
            cosine_similarities = linear_kernel(tfidf_matrix[begin:end],tfidf_matrix)
            i = 0
            for idx in range(begin,end):
                # print('---idx: %s----' %idx)
                similar_indices = cosine_similarities[i].argsort()[:-int(numtop):-1]
                similar_items = [str(path_input['movieId'][idx])]
                for j in similar_indices:
                    if(idx != j):
                        similar_items.append(str(path_input['movieId'][j])+"|"+str(cosine_similarities[i][j]))
                To_CSV(similar_items,path_output)
                i = i+1        
            index = index + 1


if __name__ == "__main__":
    # spark = SparkSession\
    # .builder\
    # .appName("Similarity-item")\
    # .getOrCreate()

    if len(sys.argv) != 3:
        log.error("+------------------------------------------------------+")
        log.error("+-----------------------Error path---------------------+")
        log.error("+------------------------------------------------------+")
        print("Usage: similarity <path_input> <path_output>", file=sys.stderr)
        # spark-submit recommendation_engine/similarity_item.py meta-data/movie_test.csv 
        # output-data/similarity-item/result4.csv
        exit(-1)
    path_input = sys.argv[1]
    path_output = sys.argv[2]
    num = int(config.offsets)
    numtop = int(config.top_moive)

    start = time.time()
    # raw_data = pd.read_csv("data/movies_26.csv", names = ["movieId", "title", "genres"])
    # raw_data = pd.read_csv("data/data_50k.csv")
    log.info("+------------------------------------------------------+")
    log.info("+--------------------BEGIN LOAD DATA INTO PD-----------+")
    log.info("+------------------------------------------------------+")
    raw_data = pd.read_csv(path_input)
    raw_data = raw_data.replace(np.nan, '', regex=True)
    # raw_data = raw_data.dropna(subset=['directors'],inplace=True)
    # print(raw_data["genres"])
    # print(raw_data['title']+raw_data['genres'])
    # print(raw_data['genres'].str.replace('|', ' ').head())
    # x = raw_data['genres'].str.replace('|', '') +' '+ raw_data['title']
    # print(x[1:].values.astype('U'))
    # print(x)
    log.info("+------------------------------------------------------+")
    log.info("+---Training data ingested in %s seconds---+" %(time.time() - start))
    log.info("+------------------------------------------------------+")
    # print("Training data ingested in %s seconds." % (time.time() - start))
    start = time.time()
    log.info("+------------------------------------------------------+")
    log.info("+------------BEGIN TRAIN MODEL-------------------------+")
    log.info("+------------------------------------------------------+")
    _train( raw_data, path_output, num, numtop)
    log.info("+------------------------------------------------------+")
    log.info("+------------------SUCCESSFULLY------------------------+")
    log.info("+--Engine trained in %s seconds.------------------+" % (time.time() - start))
    # print("Engine trained in %s seconds." % (time.time() - start))
    
    pass

import sys
import os
import logging
import time
from datetime import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json
import csv
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
# sys.path.insert(0, '/home/trantu/Desktop/engine_recommendation.git/trunk/configure/')
# from configureManager import baseConfig
from configure.configureManager import collaborativeConfig
config = collaborativeConfig()

logging.basicConfig(filename=str(config.path_log_update)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('update-collaborative')
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


KEYSPACE_IMDB_MOVIE = "db"

def getSession(keySpaceName):
    # o cong ty
    # cluster = Cluster(['10.88.113.74'])
    # o nha
    cluster = Cluster([config.host_name])
    session = cluster.connect()
    log.info("+------------------------------------------------------+")
    log.info("+-------------------creating keyspace------------------+")
    log.info("+------------------------------------------------------+")
    try:
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
            """ % keySpaceName)
        log.info("+--------------------------------------------------+")
        log.info("+---------------keyspace created successfully------+")
        log.info("+--------------------------------------------------+")
        
    except:
        log.info("+--------------------------------------------------+")
        log.info("+---------keyspace has been created already!-------+")
        log.info("+--------------------------------------------------+")
    session.set_keyspace(keySpaceName)
    return session

def insertResultCollaborativeFiltering(path_input):
    with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE_IMDB_MOVIE)
        log.info("+-----------Get session successfully-----------+")
        session.execute("TRUNCATE cfmodel")
        log.info("+----------------------------------------------+")
        log.info("+----------Begin insert collaborative-result-------+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO cfmodel (
                idx_user, 
                recommendations
                )
            VALUES (
                %(idx_user)s, 
                %(recommendations)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            index_user = int(i[0])
            recommendations = i[1:]
            # string =''
            array = []
            for j in recommendations:
                array.append(j)
                # string = string + j + ','
            # string = string[:len(string)-1]
            session.execute(
                query, 
                dict(
                    idx_user = index_user, 
                    recommendations = array
                ))

        log.info("+--------------------------------------------------------+")
        log.info("+----Insert data into collaborative-result successfully--+")
        log.info("+--------------------------------------------------------+")
    pass
"""
spark-submit engine_recommendation/updateCollaborative.py output-data/collaborative-filtering/result.csv
"""
if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("update-collaborative-result")\
    .getOrCreate()
    if len(sys.argv) !=2:
        log.info('spark-submit engine_recommendation/collaborative_filtering.py\
                            output-data/collaborative-filtering/result.csv')
        exit(-1)
    path_input1 = sys.argv[1]
    insertResultCollaborativeFiltering(path_input1)
    # insertMovieFromFileText(path_input1)

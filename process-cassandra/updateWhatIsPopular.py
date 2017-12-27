import sys
import os
import logging
import time
from datetime import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
import json
import csv

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
# sys.path.insert(0, '/home/trantu/Desktop/engine_recommendation.git/trunk/configure/')
# from configureManager import baseConfig
from configure.configureManager import whatIsPopularConfig
config = whatIsPopularConfig()

logging.basicConfig(filename=str(config.path_log_update)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('update-what-is-popular')
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


KEYSPACE_IMDB_MOVIE = str(config.keyspace)

def getSession(keySpaceName):
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

def insertResultWhatIsPopular(path_input):
    with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE_IMDB_MOVIE)
        log.info("+-----------Get session successfully-----------+")
        session.execute("TRUNCATE wipmodel")
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into imdb_what_is_popular---+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO wipmodel (
                movie_id,
                bucket, 
                views
                )
            VALUES (
                %(movie_id)s,
                %(bucket)s,
                %(views)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            movie_id =i[0]
            views = int(i[1])
            session.execute(
                query, 
                dict(
                    movie_id=movie_id,
                    bucket=0, 
                    views=views
                ))
        log.info("+--------------------------------------------------------+")
        log.info("+--Insert data into imdb_what_is_popular successfully----+")
        log.info("+--------------------------------------------------------+")

    pass

if __name__ == "__main__":

    # spark = SparkSession \
    # .builder \
    # .appName("update-what-is-popular-result") \
    # .getOrCreate()

    if len(sys.argv) !=2:
        log.info('spark-submit process-cassandra/updateWhatIsPopular.py output-data/what-is-popular/result.csv')
        exit(-1)
    path_input1 = sys.argv[1]
    insertResultWhatIsPopular(path_input1)
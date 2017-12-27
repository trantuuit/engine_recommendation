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
from configure.configureManager import specificProfileConfig
config = specificProfileConfig()

logging.basicConfig(filename=str(config.path_log_update)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('update-specific-profile')
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

def insertResultSpecificProfile(path_input):
    array=['ActorReulst','WriterReulst','GenresReulst','DirectorReulst']
    session = getSession(KEYSPACE_IMDB_MOVIE)
    # session.execute("TRUNCATE specific_profile_model")
    log.info("+-----------Get session successfully-----------+")
    log.info("+----------------------------------------------+")
    for row in array:
        with open(os.path.join(path_input, row + '.csv'), "r") as csvfile:
        # with open(path_input, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            log.info("+---Begin insert data into specific_profile_model---+")
            log.info("+----------------------------------------------+")
            query = SimpleStatement("""
                INSERT INTO specific_profile_model (
                    identifier, 
                    recommendations
                    )
                VALUES (
                    %(identifier)s, 
                    %(recommendations)s
                    )
                """, consistency_level=ConsistencyLevel.ONE)
            for i in reader:
                # print(i)
                identifier = i[0]
                recommendations = i[1].split('|')
                # print(recommendations)
                array = []
                for j in recommendations:
                    array.append(j.replace(':','|'))
                session.execute(
                    query, 
                    dict(
                        identifier=identifier, 
                        recommendations=array
                    ))
            log.info("+--------------------------------------------------------+")
            log.info("+-----Insert data into specific_profile_model successfully----+")
            log.info("+--------------------------------------------------------+")
    pass

if __name__ == '__main__':
    # spark = SparkSession \
    # .builder \
    # .appName("update-specific-profile") \
    # .getOrCreate()
    if len(sys.argv) != 2:
        print("Usage: updateSpecificProfile <input>")

        exit(-1)
    path_input1 = sys.argv[1]
    insertResultSpecificProfile(path_input1)
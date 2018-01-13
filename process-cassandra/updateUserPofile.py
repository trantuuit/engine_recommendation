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
from configure.configureManager import userProfileConfig
config = userProfileConfig()

logging.basicConfig(filename=str(config.path_log_update)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('update-user-profile')
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

def insertGenresUserProfile(path_input):
    # array=['ActorProfileReulst','WriterProfileReulst','GenresProfileReulst','DirectorProfileReulst']
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")
    with open(os.path.join(path_input, 'GenresProfileResult' + '.csv'), "r") as csvfile:
    # with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into genres_profile_model---+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO genres_profile_model (
                idx_user, 
                recommendations
                )
            VALUES (
                %(idx_user)s, 
                %(recommendations)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            index_user = i[0]
            recommendations = i[1].split('|')
            array = []
            for j in recommendations:
                array.append(j.replace(':','|'))
            session.execute(
                query, 
                dict(
                    idx_user=int(index_user), 
                    recommendations=array
                ))
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into genres_profile_model successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def insertActorsUserProfile(path_input):
    # array=['ActorProfileReulst','WriterProfileReulst','GenresProfileReulst','DirectorProfileReulst']
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")
    with open(os.path.join(path_input, 'ActorsProfileResult' + '.csv'), "r") as csvfile:
    # with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into actors_profile_model---+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO actors_profile_model (
                idx_user, 
                recommendations
                )
            VALUES (
                %(idx_user)s, 
                %(recommendations)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            index_user = i[0]
            recommendations = i[1].split('|')
            array = []
            for j in recommendations:
                array.append(j.replace(':','|'))
            session.execute(
                query, 
                dict(
                    idx_user=int(index_user), 
                    recommendations=array
                ))
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into actors_profile_model successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def insertWritersUserProfile(path_input):
    # array=['ActorProfileReulst','WriterProfileReulst','GenresProfileReulst','DirectorProfileReulst']
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")
    with open(os.path.join(path_input, 'WriterProfileResult' + '.csv'), "r") as csvfile:
    # with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into writers_profile_model---+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO writers_profile_model (
                idx_user, 
                recommendations
                )
            VALUES (
                %(idx_user)s, 
                %(recommendations)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            index_user = i[0]
            recommendations = i[1].split('|')
            array = []
            for j in recommendations:
                array.append(j.replace(':','|'))
            session.execute(
                query, 
                dict(
                    idx_user=int(index_user), 
                    recommendations=array
                ))
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into writers_profile_model successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def insertDirectorsUserProfile(path_input):
    # array=['ActorProfileReulst','WriterProfileReulst','GenresProfileReulst','DirectorProfileReulst']
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")
    with open(os.path.join(path_input, 'DirectorProfileResult' + '.csv'), "r") as csvfile:
    # with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into directors_profile_model---+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO directors_profile_model (
                idx_user, 
                recommendations
                )
            VALUES (
                %(idx_user)s, 
                %(recommendations)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for i in reader:
            index_user = i[0]
            recommendations = i[1].split('|')
            array = []
            for j in recommendations:
                array.append(j.replace(':','|'))
            session.execute(
                query, 
                dict(
                    idx_user=int(index_user), 
                    recommendations=array
                ))
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into directors_profile_model successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

if __name__ == '__main__':

    # spark = SparkSession \
    # .builder \
    # .appName("update-user-profile") \
    # .getOrCreate()

    if len(sys.argv) != 2:
        print("Usage: updateUserProfile <input>")
        """
        spark-submit process-cassandra/updateUserPofile.py output-data/user-profile/
        """
        exit(-1)
    path_input1 = sys.argv[1]
    insertGenresUserProfile(path_input1)
    insertActorsUserProfile(path_input1)
    insertWritersUserProfile(path_input1)
    insertDirectorsUserProfile(path_input1)
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
from configure.configureManager import updateTrendingConfig
config = updateTrendingConfig()

logging.basicConfig(filename=str(config.update_trending)+datetime.now().strftime('%Y_%m_%d_%H_%M_%S.log'),level=logging.DEBUG)
# logging.basicConfig(filename=datetime.now().strftime('%Y_%m_%d.log'),level=logging.DEBUG)

log = logging.getLogger('update-trending')
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
def getValue(value):
    
    try:
        return str(value)
    except UnicodeEncodeError:
        # obj is unicode
        return unicode(value).encode('unicode_escape')

    # if str(value) == 'None':
    #     return ''
    # else:
    #     return value.encode('utf-8', 'ignore').decode('utf-8')
        # return u' '.join(value).encode('utf-8').strip()
        # return value
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

def insertTrendingMovieFromFileText(path_input):
    with open(path_input, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE_IMDB_MOVIE)
        log.info("+-----------Get session successfully-----------+")
        log.info("+----------------------------------------------+")
        log.info("+---Begin insert data into TrendModel----------+")
        log.info("+----------------------------------------------+")
        session.execute("TRUNCATE trend_model")
        queryinsert = SimpleStatement("""
            INSERT INTO trend_model (
                bucket,
                rank,
                movie_id 
                )
            VALUES (
                %(bucket)s,
                %(rank)s,
                %(movie_id)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        i = 0
        for row in reader:
            if i != 0:
                rank = int(row[0])
                movie_id = str(row[1])
                session.execute(
                    queryinsert, 
                    dict(
                        bucket=0,
                        rank=rank,
                        movie_id=str(movie_id)
                    ))
                # query = "SELECT * FROM trend_model WHERE movie_id=%s"
                # record = session.execute(query, parameters=[movie_id])
                # print(record)
                # if not record:
                #     session.execute(
                #         queryinsert, 
                #         dict(
                #             movie_id=str(movie_id)
                #         ))
                # else:
                #     query_delete = "DELETE FROM trend_model WHERE movie_id=%s"
                #     session.execute(query_delete,parameters=[movie_id])
                #     pass
            i = i + 1
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into TrendModel successfully----+")
        log.info("+--------------------------------------------------------+")
    pass


def insertMovieFromFileText(path_input):
    # Id, 0
    # Title, 1
    # Year, 2
    # Genres, 3
    # Directors, 4
    # Writers, 5
    # Actors, 6
    # Countries, 7
    # ReleaseDate, 8
    # ReleaseDate1, 9
    # ReleaseDate2, 10
    # Runtime, 11
    # Rating, 12
    # RatingCount, 13
    # Popularity, 14
    # MetaScore, 15
    # PeopleMayLike, 16
    # Keywords, 17
    # Link, 18
    # Description, 19
    # image_urls, 20
    # file_urls, 21
    # image_paths 22
    
    moviesRDD = lines.map(lambda p: Row(
        movieId=getValue(str(p[1])),
        title=getValue(str(p[2])),
        genres=getValue(str(p[4])),
        directors=getValue(str(p[5])),
        writers=getValue(str(p[6])),
        actors=getValue(str(p[7])),
        description=getValue(str(p[20])),
        year=getValue(str(p[3])),
        countries=getValue(str(p[8])),
        release=getValue(str(p[10])),
        runtime=getValue(str(p[12])),
        rating=getValue(str(p[13])),
        keywords=getValue(str(p[18])),
        poster=str(p[21]),
        slate=str(p[22])
        ))
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")

    log.info("+----------------------------------------------+")
    log.info("+----------Begin insert data into movies-------+")
    log.info("+----------------------------------------------+")
    query = SimpleStatement("""
        INSERT INTO movie_model (
            idx,
            movie_id, 
            title, 
            genres, 
            directors, 
            writers, 
            actors, 
            description,
            year,
            countries,
            release,
            runtime,
            rating,
            keywords,
            poster, 
            slate
            )
        VALUES (
            %(idx)s,
            %(movie_id)s, 
            %(title)s, 
            %(genres)s, 
            %(directors)s, 
            %(writers)s, 
            %(actors)s, 
            %(description)s,
            %(year)s,
            %(countries)s,
            %(release)s,
            %(runtime)s,
            %(rating)s,
            %(keywords)s,
            %(poster)s,
            %(slate)s
            )
        """, consistency_level=ConsistencyLevel.ONE)
    result = session.execute("SELECT COUNT(*) FROM movie_model")
    count = 0
    for row in result: # will only be 1 row
        count += row.count
    # print('------total: %s' %total)
    idx = count
    for mv in moviesRDD.collect():
        qr = "SELECT * FROM movie_model WHERE movie_id=%s"
        record = session.execute(qr, parameters=[mv['movieId']])
        if not record:
            idx = idx + 1
            session.execute(
                query, 
                dict(
                    idx=idx,
                    movie_id=mv['movieId'], 
                    title=mv['title'], 
                    genres=(str(mv['genres']).split(',')), 
                    directors=(str(mv['directors']).split(',')), 
                    writers=(str(mv['writers']).split(',')), 
                    actors=(str(mv['actors']).split(',')), 
                    description=str(mv['description']),
                    year=str(mv['year']),
                    countries=(str(mv['countries']).split(',')),
                    release=str(mv['release']),
                    runtime=str(mv['runtime']),
                    rating=str(mv['rating']),
                    keywords=(str(mv['keywords']).split(',')),
                    poster=mv['poster'], 
                    slate=mv['slate']
                    ))

    log.info("+----------------------------------------------+")
    log.info("+----------End insert data into movies---------+")
    log.info("+----------------------------------------------+")
    pass

if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("run cassandra")\
    .getOrCreate()
    if len(sys.argv) !=2:
        log.info('spark-submit engine_recommendation/updateTrending.py\
                               trending.csv')
        exit(-1)
    path_input1 = sys.argv[1]
    insertTrendingMovieFromFileText(path_input1)
    # insertMovieFromFileText(path_input1)

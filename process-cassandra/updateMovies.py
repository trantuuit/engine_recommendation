import sys
import logging
import time
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json

def getValue(value):
    if str(value) == 'None':
        return ''
    else:
        # return value.encode('utf-8')
        return value

# KEYSPACE_IMDB_MOVIE = "imdb_movie"
KEYSPACE_IMDB_MOVIE = "db"
#idx,
# movieId,
# title,
# genres,
# directors,
# writers,
# actors,
# description,
# year,
# countries,
# release,
# runtime,
# rating,
# keywords,
# poster,
# slate
def insertMovieFromFileText(path_input):
    lines = spark.read.format('com.databricks.spark.csv')\
    .option("header", True)\
    .load(path_input).rdd
    
    moviesRDD = lines.map(lambda p: Row(
        idx=str(p[0]),
        movieId=getValue(str(p[1])),
        title=getValue(str(p[2])),
        genres=getValue(str(p[3])),
        directors=getValue(str(p[4])),
        writers=getValue(str(p[5])),
        actors=getValue(str(p[6])),
        description=getValue(str(p[7])),
        year=getValue(str(p[8])),
        countries=getValue(str(p[9])),
        release=getValue(str(p[10])),
        runtime=getValue(str(p[11])),
        rating=getValue(str(p[12])),
        keywords=getValue(str(p[13])),
        poster=str(p[14]),
        slate=str(p[15])
        ))
    # print('deo hieu')
    # print(moviesRDD)
    # print(moviesRDD.collect())
    session = getSession(KEYSPACE_IMDB_MOVIE)
    log.info("+-----------Get session successfully-----------+")
    # session.execute("DROP TABLE IF EXISTS imdb_movie")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE imdb_movie successfully-----+")
    # log.info("+----------------------------------------------+")

    # log.info("+----------------------------------------------+")
    # log.info("+---------------creating table-----------------+")
    # log.info("+----------------------------------------------+")
    # session.execute("""
    #     CREATE TABLE IF NOT EXISTS imdb_movie (
    #         movieId text PRIMARY KEY,  
    #         title text, 
    #         genres list<text>, 
    #         directors list<text>, 
    #         writers list<text>, 
    #         actors list<text>, 
    #         description text,
    #         poster text,
    #         slate text  
    #     )
    #     """)

    log.info("+----------------------------------------------+")
    log.info("-------------table created successfully--------+")
    log.info("+----------------------------------------------+")

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
                # 
    # 
    for mv in moviesRDD.collect():
        # print(str(mv['slate']))
        session.execute(
            query, 
            dict(
                idx=int(mv['idx']),
                movie_id=mv['movieId'], 
                title=mv['title'], 
                genres=(str(mv['genres']).split('|')), 
                directors=(str(mv['directors']).split('|')), 
                writers=(str(mv['writers']).split('|')), 
                actors=(str(mv['actors']).split('|')), 
                description=str(mv['description']),
                year=str(mv['year']),
                countries=(str(mv['countries']).split('|')),
                release=str(mv['release']),
                runtime=str(mv['runtime']),
                rating=str(mv['rating']),
                keywords=(str(mv['keywords']).split('|')),
                poster=str(mv['poster']), 
                slate=str(mv['slate'])
                ))
        pass

    log.info("+----------------------------------------------+")
    log.info("+----------End insert data into movies---------+")
    log.info("+----------------------------------------------+")
    pass

def getSession(keySpaceName):
    cluster = Cluster(['127.0.0.1'])
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
if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("run cassandra")\
    .getOrCreate()
    if len(sys.argv) !=2:
        exit(-1)
    t0 = time.time()
    path_input1 = sys.argv[1]
    insertMovieFromFileText(path_input1)

    log.info("+----------------------------------------+")
    log.info("+---------------Take time:%s-------------+"%round(time.time()-t0,2))
    log.info("+----------------------------------------+")
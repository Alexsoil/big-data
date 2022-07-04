from importlib.metadata import metadata
import connect_db
import pandas as pd
import os
from cassandra import cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from tqdm import tqdm


try:
    con = connect_db.connect()
    session = con[0]
    clstr = con[1]
    data = cluster.Metadata.keyspaces
    print(data)
    # rows = session.execute("select * from movies_by_rating;")
    # for row in rows:
    #     print ('MovieID: {id} Title:{title} Rating:{score} Timestamp:{date}'.format(id=row[0], title=row[3], score=row[2], date=row[1]))

    row = session.execute("DROP TABLE IF EXISTS movies_by_rating;")
    row = session.execute("CREATE TABLE movies_by_rating (movieId int, title text, rating float, stamp timestamp, PRIMARY KEY ((movieId), stamp) ) WITH comment = 'Q1. Find best rated movies on specific timeframe' AND CLUSTERING ORDER BY (stamp DESC);")
    print(row)
    query = "INSERT INTO movies_by_rating (movieId, title, rating, stamp) VALUES (?, ?, ?, ?)"
    # batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q1_table.pkl")
    for idx, row in Q.iterrows():
        session.execute(prepared, (row[0], row[1], row[2], row[3]))

    row = session.execute("DROP TABLE IF EXISTS movies_by_keyword;")
    row = session.execute("CREATE TABLE movies_by_keyword (movieId int, title text, rating float, PRIMARY KEY ((movieId), title) ) WITH comment = 'Q2. Find movies by using keywords';")
    print(row)
    query = "INSERT INTO movies_by_keyword (movieId, title, rating, stamp) VALUES (?, ?, ?)"
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q2_table.pkl")
    for idx, row in Q.iterrows():
        session.execute(prepared, (row[0], row[1], row[2]))

    row = session.execute("DROP TABLE IF EXISTS movies_by_genre_rating;")
    row = session.execute("CREATE TABLE movies_by_genre_rating (movieId int, title text, genres set, rating float, PRIMARY KEY ((movieId), genres, rating) ) WITH comment = 'Q3_1. Find movies by genre, sorted by rating' AND CLUSTERING ORDER BY (genres ASC, rating DESC);")
    print(row)
    query = "INSERT INTO movies_by_genre_rating (movieId, title, genres, rating) VALUES (?, ?, ?, ?)"
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q3_1_table.pkl")
    for idx, row in Q.iterrows():
        session.execute(prepared, (row[0], row[1], row[2], row[3]))

    row = session.execute("DROP TABLE IF EXISTS movies_by_genres(release_date);")
    row = session.execute("CREATE TABLE movies_by_genres(release_date) (movieId int, title text, genres text, date int, PRIMARY KEY ((movieId), genres, date) ) WITH comment = 'Q3_2. Find movies by genre, sorted by release date' AND CLUSTERING ORDER BY (genres ASC, date DESC);")
    print(row)
    query = "INSERT INTO movies_by_genre_rating (movieId, title, genres, date) VALUES (?, ?, ?, ?)"
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q3_2_table.pkl")
    for idx, row in Q.iterrows():
        session.execute(prepared, (row[0], row[1], row[2], row[3]))

    row = session.execute("DROP TABLE IF EXISTS movie_info;")
    row = session.execute("CREATE TABLE movie_info (movieId int, title text, genres set, rating float, tag text, PRIMARY KEY ((movieId), tag) ) WITH comment = 'Q4. View more info about a movie';")
    print(row)
    query = "INSERT INTO movies_by_genre_rating (movieId, title, rating, tag) VALUES (?, ?, ?, ?, ?)"
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q4_table.pkl")
    for idx, row4 in Q.iterrows():
       session.execute(prepared, (row[0], row[1], row[2], row[3], row[4]))

    row = session.execute("DROP TABLE IF EXISTS movies_by_tag;")
    row = session.execute("CREATE TABLE movies_by_tag (movieId int, title text, rating float, tag text, PRIMARY KEY ((movieId), tag) ) WITH comment = 'Q5. View movies relevant to a tag' AND CLUSTERING ORDER BY (tag ASC);")
    print(row)
    query = "INSERT INTO movies_by_genre_rating (movieId, title, rating, tag) VALUES (?, ?, ?, ?)"
    prepared = session.prepare(query)
    Q = pd.read_pickle("pickleJar" + os.path.sep + "Q5_table.pkl")
    for idx, row4 in Q.iterrows():
       session.execute(prepared, (row[0], row[1], row[2], row[3]))



    connect_db.disconnect(clstr)
    exit()  
except Exception as e:
    print(e)
    print("H porta kollhse")
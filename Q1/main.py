from importlib.metadata import metadata
import connect_db
import pandas as pd
import os
from cassandra import cluster


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
    row = session.execute("CREATE TABLE movies_by_rating (movieId int, title text, rating float, stamp timestamp, PRIMARY KEY ((movieId), stamp) ) WITH comment = 'Q1. Find best rated movies on specific timeframe';")
    print(row)
    Q1 = pd.read_pickle("pickleJar" + os.path.sep + "Q1_table.pkl")
    for idx, row in Q1.iterrows():
        query = "insert into movies_by_rating (movieId, title, rating, stamp) values (" + str(row['movieId']) + ", '" + str(row['title'].rstrip()) + "', " + str(row['rating']) + ", '" + str(row['timestamp']) + "');"
        session.execute(query)



    row2 = session.execute("DROP TABLE IF EXISTS movies_by_keyword;")
    row2 = session.execute("CREATE TABLE movies_by_keyword (movieId int, title text, rating float, PRIMARY KEY ((movieId), title) ) WITH comment = 'Q1. Find movies by using keywords';")
    print(row2)
    Q2 = pd.read_pickle("pickleJar" + os.path.sep + "Q2_table.pkl")
    for idx, row2 in Q2.iterrows():
        query = "insert into movies_by_keyword (movieId, title, rating) values (" + str(row2['movieId']) + ", '" + str(row2['title'].rstrip()) + "', " + str(row2['rating']) +"');"
        session.execute(query)



    row3 = session.execute("DROP TABLE IF EXISTS movies_by_genre(rating);")
    row3 = session.execute("CREATE TABLE movies_by_genre(rating) (movieId int, title text, genres text, rating float, PRIMARY KEY ((movieId), genres) ) WITH comment = 'Q1. Find movies by genre, sorted by rating';")
    print(row3)
    Q3_1 = pd.read_pickle("pickleJar" + os.path.sep + "Q3_1_table.pkl")
    for idx, row3 in Q3_1.iterrows():
        query = "insert into movies_by_genre(rating) (movieId, title, genres, rating) values (" + str(row3['movieId']) + ", '" + str(row3['title'].rstrip()) + "', " + str(row3['genres']) + ", '"+ str(row3['rating']) + ", '" + "');"
        session.execute(query)



    row3_2 = session.execute("DROP TABLE IF EXISTS movies_by_genres(release_date);")
    row3_2 = session.execute("CREATE TABLE movies_by_genres(release_date) (movieId int, title text, genres text, date year, PRIMARY KEY ((movieId), genres) ) WITH comment = 'Q1. Find movies by genre, sorted by release date';")
    print(row)
    Q3_2 = pd.read_pickle("pickleJar" + os.path.sep + "Q3_2_table.pkl")
    for idx, row3_2 in Q3_2.iterrows():
        query = "insert into movies_by_genres(release_date) (movieId, title, genres, date) values (" + str(row3_2['movieId']) + ", '" + str(row3_2['title'].rstrip()) + "', " + str(row3_2['genres']) + ", '" + str(row3_2['date']) + "');"
        session.execute(query)



    #row4 = session.execute("DROP TABLE IF EXISTS movie_info;")
    #row4 = session.execute("CREATE TABLE movie_info (movieId int, title text, genres text, rating float, tag text, PRIMARY KEY ((movieId), title) ) WITH comment = 'Q1. View more info about a movie';")
    #print(row)
    #Q4 = pd.read_pickle("pickleJar" + os.path.sep + "Q4_table.pkl")
    #for idx, row4 in Q4.iterrows():
      #  query = "insert into movie_info (movieId, title, genres, rating, tag) values (" + str(row4['movieId']) + ", '" + str(row4['title'].rstrip()) + "', " + str(row4['genres']) + ", '" + str(row4['rating']) + ", '" + str(row4['tag']) + "');"
      #  session.execute(query)


    
   # row5 = session.execute("DROP TABLE IF EXISTS movies_by_tag;")
   # row5 = session.execute("CREATE TABLE movies_by_tag (movieId int, title text, tag text, rating float, PRIMARY KEY ((movieId), tag) ) WITH comment = 'Q1. Find top movies by tag';")
   # print(row)
   # Q5 = pd.read_pickle("pickleJar" + os.path.sep + "Q5_table.pkl")
   # for idx, row5 in Q5.iterrows():
       # query = "insert into movies_by_tag (movieId, title, tag, rating) values (" + str(row5['movieId']) + ", '" + str(row5['title'].rstrip()) + "', " + str(row5['tag']) + ", '" + str(row5['rating']) + "');"
       # session.execute(query)




    connect_db.disconnect(clstr)
    exit()  
except Exception as e:
    print(e)
    print("H porta kollhse")
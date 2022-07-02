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
    connect_db.disconnect(clstr)
    exit()  
except Exception as e:
    print(e)
    print("H porta kollhse")
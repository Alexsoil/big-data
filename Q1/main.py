from importlib.metadata import metadata
import connect_db
from cassandra import cluster

try:
    con = connect_db.connect()
    session = con[0]
    clstr = con[1]
    data = cluster.Metadata.keyspaces
    print(data)
    print("Please, I beg you")
    connect_db.disconnect(clstr)
    print("Yo I'm adding this guy to friends`")
    
except Exception as e:
    print(e)
    print("H porta kollhse")
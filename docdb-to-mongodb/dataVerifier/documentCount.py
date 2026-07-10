import pymongo
from pymongo import MongoClient
from bson import json_util
import json
import hashlib

# DocumentDB connection with proper TLS configuration
clientSource = pymongo.MongoClient(
    "mongodb://whos:who@my-docdb.cluster-xyz.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=primary&retryWrites=false",
    tls=True,
    tlsCAFile="/PATH/TO/global-bundle.pem",
    tlsAllowInvalidHostnames=False
)

# MongoDB Atlas connection
clientTarget = pymongo.MongoClient("mongodb+srv://whos:who@abcdef.ghijkl.mongodb.net/?retryWrites=true&w=majority")

dbInput = "samquote"
colInput = [ "quotes", "tasks"]

for coll in colInput:    
    dbSource = clientSource[dbInput]
    dbTarget = clientTarget[dbInput]

    colSource = dbSource[coll]
    colTarget = dbTarget[coll]

    print('-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----')
    print(f'Start counting documents on Source Collection: {coll}')
    cursorSource = colSource.count_documents({},hint=[('_id', 1)])
    print(f'Total number of documents: {str(cursorSource)}')

    print(f'Start counting documents on Target Collection: {coll}')
    cursorTarget = colTarget.count_documents({},hint=[('_id', 1)])
    print(f'Total number of documents: {str(cursorTarget)}')
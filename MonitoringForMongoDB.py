from pymongo import MongoClient
import graphyte
import json
import math

graphyte.init('graphite.dev-i.net', prefix='mongo')
client = MongoClient('localhost', 27017)
dbs = client.database_names()

count = len(dbs)
graphyte.send('dbs_count', count)

for db in dbs:
    dbstat = client[db].command("dbstats")
    
    db_dataSize = (dbstat['dataSize'])
    db_indexSize = (dbstat['indexSize'])
    db_storageSize = (dbstat['storageSize'])

    graphyte.send('db_dataSize', db_dataSize)
    graphyte.send('db_indexSize', db_indexSize)
    graphyte.send('db_storageSize', db_storageSize)
    
    collections = client[db].collection_names()

    for y in collections:
        colstat = client[db].command("collstats", y)

        col_size = (colstat['size'])
        col_count = (colstat['count'])
        col_storageSize = (colstat['storageSize'])
        col_totalIndexSize = (colstat['totalIndexSize'])

        graphyte.send('col_size', col_size)
        graphyte.send('col_count', col_count)
        graphyte.send('col_storageSize', col_storageSize)
        graphyte.send('col_totalIndexSize', col_totalIndexSize)

dbstop = client.admin.command("top")
dump = json.dumps(dbstop, indent=4)

readTotal = 0
writeTotal = 0

new_dict_read = {}
new_dict_write = {}

for col_time in dbstop["totals"]:
    if "readLock" in dbstop["totals"][col_time]:
        new_dict_read[col_time] = dbstop["totals"][col_time]["readLock"]["time"]
        readTotal = readTotal + dbstop["totals"][col_time]["readLock"]["time"]
    if "writeLock" in dbstop["totals"][col_time]:
        new_dict_write[col_time] = dbstop["totals"][col_time]["writeLock"]["time"]
        writeTotal = writeTotal + dbstop["totals"][col_time]["writeLock"]["time"]

read = readTotal/1000000
write = writeTotal/1000000
r = math.ceil(read)
w = math.ceil(write)

graphyte.send('readLock', r)
graphyte.send('writeLock', w)

col_number = sum(len(s) for s in collections)
graphyte.send('col_number', col_number)

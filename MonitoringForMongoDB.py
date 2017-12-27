#!/usr/bin/python

### File            : mongo_status.py
### Description     : Monitoring MongoDB and sending metrics to Graphyte
### Author          : Oleh Sharudin
### Version history :
### 20171128 1.0 Basic idea was created
### 20171215 1.1 Script reday for use
###
### Needed libs: pymongo, graphyte, sys, math, argparse, logging
###
### pip install pymongo; pip install graphyte; pip install sys; pip install json; pip install math; pip install argparse; pip install logging
###
### Example of use - python MongoStatus.py -mongo_host localhost -mongo_port 27017 -graphyte_host graphite.dev-i.net -graphyte_prefix mongo
###
### { "mongo_host":"127.0.0.1", "mongo_port":27017, "graphyte_host":"graphite.dev-i.net", "graphyte_prefix":"mongo" }
###

from pymongo import MongoClient
import graphyte
import math
import sys
import json
import argparse
import logging

gi_verbose=0

def print_errmsg (err0,err1):
#    print "Status:501\n Content-type: text/html\n"
    if err0 != "" :
        print ("--------------------------------------------------------------------------------------------------------------------------------------")
        print (err0)
        logging.error (err0)
    if err1 != "" :
        print ("--------------------------------------------------------------------------------------------------------------------------------------")
        print (err1)
        logging.error (err1)
    print ("--------------------------------------------------------------------------------------------------------------------------------------")
    sys.exit()

def print_infomsg (fs_info):
    logging.info (fs_info)
    if gi_verbose > 0 :
        print (fs_info)


commandLineArgumentParser = argparse.ArgumentParser(description='Provide host name and port for MongoDB + host name and port for Graphyte')
commandLineArgumentParser.add_argument("-mongo_host", "--mongo_host",  help="127.0.0.1")
commandLineArgumentParser.add_argument("-mongo_port","--mongo_port", help="27017")
commandLineArgumentParser.add_argument("-graphyte_host","--graphyte_host", help="")
commandLineArgumentParser.add_argument("-graphyte_prefix","--graphyte_prefix", help="")
commandLineArgumentParser.add_argument("-conffile","--conffile", help="/etc/onseo/mongostatus.json")
commandLineArgumentParser.add_argument("-logfile","--logfile", help="/var/log/mongostatus/mongo_status.log")
commandLineArgumentParser.add_argument("-verbose","--verbose", help="")
commandLineArgumentParser.add_argument("-test","--test", help="")
commandLineArguments = commandLineArgumentParser.parse_args()

if commandLineArguments.verbose is None :
    gi_verbose=0
else:
    gi_verbose=int(commandLineArguments.verbose)

if commandLineArguments.test is None :
    gb_test=False
else:
    gb_test=True

if commandLineArguments.conffile is None :
    g_conffile = ""
else:
    g_conffile = commandLineArguments.conffile

if commandLineArguments.logfile is None :
    gs_logfilepath="/var/log/mongostatus/mongo_status.log"
else:
    gs_logfilepath = commandLineArguments.logfile

logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s', level = logging.DEBUG, filename = u'mongo_status.log', filemode="w")

print_infomsg('Program has been started')

if len(g_conffile) > 0 :
    try:
        with open(g_conffile) as data_file:
            g_ms_config = json.load(data_file)

        mongo_host = g_ms_config ["mongo_host"]
        mongo_port = int(g_ms_config ["mongo_port"])
        graphyte_host = g_ms_config ["graphyte_host"]
        graphyte_prefix = g_ms_config ["graphyte_prefix"]
    except Exception as err:
        print_errmsg ("Load config file #00",err)
else:
    try:
        mongo_host = commandLineArguments.mongo_host
        mongo_port = int(commandLineArguments.mongo_port)
        graphyte_host = commandLineArguments.graphyte_host
        graphyte_prefix = commandLineArguments.graphyte_prefix
    except Exception as err:
        print_errmsg ("Load commandline args #00",err)

if len (mongo_host) == 0 :
    print_errmsg ("Empty mongo_host #00a","")

if len (graphyte_host) == 0 :
    print_errmsg ("Empty graphyte_host #00c","")
if len (graphyte_prefix) == 0 :
    print_errmsg ("Empty graphyte_prefix #00d","")


print_infomsg ( u"Config loaded!")
print_infomsg ( u"Logs for the "+graphyte_prefix)


try:
    g_mclient = MongoClient(mongo_host, mongo_port) #connection to MongoDB
    
except Exception as err:
    print_errmsg ("Unable open server connect #02",err)

print_infomsg ("DB connected!")

gas_dbs = g_mclient.database_names()
interval_time = math.ceil((len(gas_dbs))/2)


try:
    graphyte.init(graphyte_host, prefix=graphyte_prefix, log_sends=True, interval=interval_time) # Graphite settings initialization
except Exception as err:
    print_errmsg ("graphyte.init #01",err)


print_infomsg ("Collecting mongo metrics DB - START")

print_infomsg ( u"Metrics will be sent to the "+graphyte_host)

for gs_db in gas_dbs:
    print_infomsg ("        Process "+ str(gs_db))
    collections = g_mclient[gs_db].collection_names()

    for gs_col in collections:
        print_infomsg ("            Process "+ str(gs_col))

        colstat = g_mclient[gs_db].command("collstats", gs_col)

        col_size = (colstat['size'])
        col_count = (colstat['count'])
        col_storageSize = (colstat['storageSize'])
        col_totalIndexSize = (colstat['totalIndexSize'])

        if not gb_test :
            print_infomsg ("Send data to graphyte - START")
            print_infomsg ("Collecting mongo collections size - START")
            graphyte.send("collections."+gs_db+"."+gs_col+'.col_size', col_size) #sending to graphyte collections size
            print_infomsg ("Collecting mongo collections size - END")
            print_infomsg ("Collecting mongo collections count - START")
            graphyte.send("collections."+gs_db+"."+gs_col+'.col_count', col_count) #sending to graphyte collections count
            print_infomsg ("Collecting mongo collections count - END")
            print_infomsg ("Collecting mongo collections storageSize - START")
            graphyte.send("collections."+gs_db+"."+gs_col+'.col_storageSize', col_storageSize) #sending to graphyte collections storageSize
            print_infomsg ("Collecting mongo collections storageSize - END")
            print_infomsg ("Collecting mongo collections totalIndexSize - START")
            graphyte.send("collections."+gs_db+"."+gs_col+'.col_totalIndexSize', col_totalIndexSize) #sending to graphyte collections totalIndexSize
            print_infomsg ("Collecting mongo collections totalIndexSize - END")



print_infomsg (u"Collecting mongo TOP usage statistics")
g_top = g_mclient.admin.command("top") # Top command returns usage statistics for each collection
dump = json.dumps(g_top, indent=4)


#topjson_filepath="/var/log/mongostatus/mongo_top.json" #combat path to mongo_top.json file
topjson_filepath="C:/Users/osharudin.SEO/Desktop/MongoStatus/mongo_top.json" #test path to mongo_top.json file

try:
    print_infomsg (u"reading Top command output from the JSON file")
    with open(topjson_filepath, 'r') as data_file:
        json_dbstop = json.load(data_file)
except Exception as err:
    print_infomsg("No json file with top command output")
    with open(topjson_filepath, 'w') as outfile:
        json.dump(g_top, outfile)
    sys.exit()    


json_read_time = 0
json_write_time = 0
top_read_time = 0
top_write_time = 0


print_infomsg (u"Writing Top command output to the JSON file:"+dump)
with open(topjson_filepath, 'w') as outfile:
    json.dump(g_top, outfile)


for json_item in json_dbstop["totals"]: #reading JSON file with readLock, writeLock time
    if "readLock" in json_dbstop["totals"][json_item]:
        json_read_time = json_dbstop["totals"][json_item]["readLock"]["time"]

    if "writeLock" in json_dbstop["totals"][json_item]:
        json_write_time = json_dbstop["totals"][json_item]["writeLock"]["time"]

    if "readLock" in g_top["totals"][json_item]:
        top_read_time = g_top["totals"][json_item]["readLock"]["time"]
 
    if "writeLock" in g_top["totals"][json_item]:
        top_write_time = g_top["totals"][json_item]["writeLock"]["time"]

    
    print ("json_write_time - "+ str(json_write_time))
    print ("json_read_time - "+ str(json_read_time))
    print ("top_write_time - "+ str(top_write_time))
    print ("top_read_time - "+ str(top_read_time))

    print_infomsg (u"Comparing Top command output and JSON file data")
    read = (json_read_time + top_read_time)
    read_time = math.ceil(read/1000000)
        
    write = (json_write_time + top_write_time)
    write_time = math.ceil(write/1000000)

    print ("read_sum - "+ str(read_time))
    print ("write_sum - "+ str(write_time))

    print_infomsg ("Sending to graphyte collections writeLock time - START")
    graphyte.send("collections."+json_item+"."+'writeLock', write_time) #sending to graphyte collections writeLock time
    print_infomsg ("Sending to graphyte collections writeLock time - END")
    
    print_infomsg ("Sending to graphyte collections readLock time - START")
    graphyte.send("collections."+json_item+"."+'readLock', read_time) #sending to graphyte collections readLock time
    print_infomsg ("Sending to graphyte collections readLock time - END")
    
    print ("collections."+json_item+"."+'writeLock', write_time)
    print ("collections."+json_item+"."+'readLock', read_time)
    
print_infomsg ("Collecting mongo metrics DB - END")
print_infomsg ("Send data to graphyte - END")

g_mclient.close()

print_infomsg ("DB disconnected!")

print_infomsg ("Success!")
#EOF

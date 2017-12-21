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
#interval_time = math.ceil((len(gas_dbs))/2)


try:
    graphyte.init(graphyte_host, prefix=graphyte_prefix, log_sends=True) # Graphite settings initialization
except Exception as err:
    print_errmsg ("graphyte.init #01",err)


print_infomsg ("Collecting mongo TOP usage statistics")
dbstop = g_mclient.admin.command("top") # Top command returns usage statistics for each collection
dump = json.dumps(dbstop, indent=4)
print_infomsg (u"TOP command output:"+dump)


with open('mongo_top.json', 'w') as outfile: # Writing Top command output to the json file
    json.dump(dbstop, outfile)
outfile.close()

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


with open('mongo_top.json', 'r') as data_file:    
    admin_dbstop = json.load(data_file)
            
for col_time in admin_dbstop["totals"]:
    print_infomsg ("Collecting mongo lock times - START") 
    if "readLock" in admin_dbstop["totals"][col_time]:
        read_total = admin_dbstop["totals"][col_time]["readLock"]["time"]
        read = read_total/1000000
        graphyte.send("collections."+col_time+"."+'readLock', read) #sending to graphyte collections readLock time
    if "writeLock" in admin_dbstop["totals"][col_time]:
        write_total = admin_dbstop["totals"][col_time]["writeLock"]["time"]
        write = write_total/1000000
        graphyte.send("collections."+col_time+"."+'writeLock', write) #sending to graphyte collections writeLock time
    print_infomsg ("Collecting mongo lock times  - END")
        
print_infomsg ("Collecting mongo metrics DB - END")
print_infomsg ("Send data to graphyte - END")

g_mclient.close()
data_file.close() #closing the JSON file

print_infomsg ("DB disconnected!")

print_infomsg ("Success!")
#EOF

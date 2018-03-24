__author__ = 'akashmalla'

import pymongo
import schedule
import time
import json
import traceback


def mongodb_conn():
    try:
        maxSevSelDelay=10000

	#Connect to IP address where mongoDB install
        client = pymongo.MongoClient("54.193.115.144",serverSelectionTimeoutMS=maxSevSelDelay)
        client.server_info()
    except pymongo.errors.ConnectionFailure as err1:
        print("Could not connect to server:",err1)
    except pymongo.errors.ServerSelectionTimeoutError as err2:
        print("Could not connect to server:",err2)
    return client

def job():
    #Connect MongoDB
    conn = mongodb_conn()
    print("connected successfully")
    if conn is None:
        # no connection, exit early
        print("not connected!")
        return
    try:
        db = conn.data
        input=db.current_op(True)

        for j in input['inprog']:
	# Fetch data from active object
            if j['active']==True:
                if 'microsecs_running' in j and j['microsecs_running']>100:
                        newJ=dict()
                        if 'opid' in j:
                            newJ['opid']=j['opid']
                        if 'op' in j:
                            newJ['operation']=j['op']
                        if 'microsecs_running' in j:
                            newJ['microsecs_running']=j['microsecs_running']
			if 'client' in j:
                            newJ['client']=j['client']
                        if 'namespace' in j:
                            newJ['namespace']=j['namespace']
                        if 'time' in j:
                            newJ['time']=j['time']
			if 'current_connections' in j:
                            newJ['current_connections']=j['current_connections']
                        if 'active_clients' in j:
                            newJ['active_clients']=j['active_clients']
                        if 'current_queque' in j:
                            newJ['current_queque']=j['current_queque']
			if 'network_bytesIn' in j:
                            newJ['network_bytesIn']=j['network_bytesIn']
                        if 'network_bytesOut' in j:
                            newJ['network_bytesOut']=j['network_bytesOut']
                        if 'network_numRequests' in j:
                            newJ['network_numRequests']=j['network_numRequests']
			if 'opcounters_insert' in j:
                            newJ['opcounters_insert']=j['opcounters_insert']
                        if 'opcounters_query' in j:
                            newJ['opcounters_query']=j['opcounters_query']
                        if 'opcounters_update' in j:
                            newJ['opcounters_update']=j['opcounters_update']
			if 'extra_info_page_faults' in j:
                            newJ['extra_info_page_faults']=j['extra_info_page_faults']
                        if 'memory_virtual' in j:
                            newJ['memory_virtual']=j['memory_virtual']
                        if 'memory_residence' in j:
                            newJ['memory_residence']=j['memory_residence']
    except:
        traceback.print_exc()

job()
schedule.every(1).seconds.do(job)
schedule.every().hour.do(job)
schedule.every().day.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)

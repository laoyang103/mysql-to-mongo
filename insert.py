#!/usr/bin/python

import time
import pymongo
import datetime
import MySQLdb

_id, client, server, protocol, method, url, httpreturncode, location, referer, useragent, contenttype, _bytes, begintime, endtimewithpayload, flowhash, cookie, terminator, appllatency, clientlatency, serverlatency, rtt, applicationid, application, balancerhost, serverip, rehttppkts, client2server_teid, server2client_teid, flowusername, fbookchat, port, loc_id, loc_id_cn, forward = range(34)
names = ['_id', 'client', 'server', 'protocol', 'method', 'url', 'httpreturncode', 'location', 'referer', 'useragent', 'contenttype', '_bytes', 'begintime', 'endtimewithpayload', 'flowhash', 'cookie', 'terminator', 'appllatency', 'clientlatency', 'serverlatency', 'rtt', 'applicationid', 'application', 'balancerhost', 'serverip', 'rehttppkts', 'client2server_teid', 'server2client_teid', 'flowusername', 'fbookchat', 'port', 'loc_id', 'loc_id_cn', 'forward']

conn = pymongo.Connection()
mongodb = conn.test
ipset = mongodb.ipset
dataset = mongodb.dataset
record_cached = {}

#sql = 'SELECT * FROM http_log_000000004 where server = '192.168.1.12' limit 0, 50'
mysqldb = MySQLdb.connect('localhost','root','123456','ipm' )

def insert(sql):
    cursor = mysqldb.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        print row[_id]
        timestamp = str(int(row[begintime]))
        no_point_url = row[url].replace('.', '_')
        no_point_domain = row[server].replace('.', '_')
        if -1 != no_point_url.find('?'): no_point_url = no_point_url[:no_point_url.find('?')]

        # build index tree
        ipdoc = ipset.find_one({'_id': row[serverip]})
        if not ipdoc: 
            ipdoc = {'_id': row[serverip]}
            ipset.insert(ipdoc)
        if not ipdoc.has_key(no_point_domain): 
            ipdoc[no_point_domain] = {}
            ipset.update({'_id': row[serverip]}, ipdoc)
        if not ipdoc[no_point_domain].has_key(no_point_url):
            ipdoc[no_point_domain][no_point_url] = {}
            ipset.update({'_id': row[serverip]}, ipdoc)
        tree_url = ipdoc[no_point_domain][no_point_url]

        # this url has no data or save more than 15000
        if not tree_url.has_key('curr') or tree_url[tree_url['curr']]['count'] > 15000:
            insert_id = dataset.insert({})
            tree_url['curr'] = timestamp
            tree_url[timestamp] = {'key': insert_id, 'count': 0}
        use_ts = tree_url['curr']

        # common add 
        tree_url[use_ts]['count'] += 1
        ipset.update({'_id': row[serverip]}, ipdoc)

        # fill data docment record
        fields = {}
        for i in range(34): fields[names[i]] = row[i]

        # create insert cached
        insert_id = tree_url[use_ts]['key']
        if not record_cached.has_key(insert_id):
            record_cached[insert_id] = []

        # cached or insert record to mongodb
        if len(record_cached[insert_id]) > 500: 
            dataset.update({'_id': insert_id}, {'$push': {'records': {'$each': record_cached[insert_id]}}})
            record_cached[insert_id] = []
        else:
            record_cached[insert_id].append(fields)

    # flush cached record
    for k, v in record_cached.items():
        if len(record_cached[k]) > 0: 
            print len(record_cached[k])
            dataset.update({'_id': k}, {'$push': {'records': {'$each': v}}})
            record_cached[k] = []

#sql = 'SELECT * FROM http_log_000000004 limit 0, 1000000'
#insert(sql)
#sql = 'SELECT * FROM http_log_000000005 limit 0, 1000000'
#insert(sql)
#sql = 'SELECT * FROM http_log_000000006 limit 0, 1000000'
#insert(sql)
#sql = 'SELECT * FROM http_log_000000008 limit 0, 1000000'
#insert(sql)
sql = 'SELECT * FROM http_log_000000009 limit 0, 1000000'
insert(sql)
sql = 'SELECT * FROM http_log_000000011 limit 0, 1000000'
insert(sql)
sql = 'SELECT * FROM http_log_000000012 limit 0, 1000000'
insert(sql)
sql = 'SELECT * FROM http_log_000000013 limit 0, 1000000'
insert(sql)


# write index dict to mongodb
#for i in ipdict:
#    ipset.insert(ipdict[i])

#        starttime1 = time.time()  
#        endtime1 = time.time()  
#        print endtime1 - starttime1

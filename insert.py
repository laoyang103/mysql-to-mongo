#!/usr/bin/python

import json
import time
import pymongo
import datetime
import MySQLdb

_id, client, server, protocol, method, url, httpreturncode, location, referer, useragent, contenttype, _bytes, begintime, endtimewithpayload, flowhash, cookie, terminator, appllatency, clientlatency, serverlatency, rtt, applicationid, application, balancerhost, serverip, rehttppkts, client2server_teid, server2client_teid, flowusername, fbookchat, port, loc_id, loc_id_cn, forward = range(34)
names = ["_id", "client", "server", "protocol", "method", "url", "httpreturncode", "location", "referer", "useragent", "contenttype", "_bytes", "begintime", "endtimewithpayload", "flowhash", "cookie", "terminator", "appllatency", "clientlatency", "serverlatency", "rtt", "applicationid", "application", "balancerhost", "serverip", "rehttppkts", "client2server_teid", "server2client_teid", "flowusername", "fbookchat", "port", "loc_id", "loc_id_cn", "forward"]
ips = [3720385042, 3396081437, 1875813415, 2099758888, 2099713289, 1696398356, 2008822372, 3232235788, 2071819107, 2067447099, 3396084028, 2071797212, 2099708298, 3395987888]

conn = pymongo.Connection()
mongodb = conn.test
ipset = mongodb.ipset
dataset = mongodb.dataset
ipset.drop()
dataset.drop()
ipdict = {}

sql = "SELECT * FROM http_log_000000004 limit 0, 1000000"
#sql = "SELECT * FROM http_log_000000004 where server = '192.168.1.12' limit 0, 10"
mysqldb = MySQLdb.connect("localhost","root","123456","ipm" )

def insert():
    cursor = mysqldb.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        print row[_id]
        timestamp = str(int(row[begintime]))
        no_point_url = row[url].replace('.', '_')
        no_point_domain = row[server].replace('.', '_')
        if -1 != no_point_url.find('?'): no_point_url = no_point_url[:no_point_url.find('?')]

        # build index docment
        if not ipdict.has_key(row[serverip]):
            ipdict[row[serverip]] = {}
            ipdict[row[serverip]]['_id'] = row[serverip]
        if not ipdict[row[serverip]].has_key(no_point_domain):
            ipdict[row[serverip]][no_point_domain] = {}
        if not ipdict[row[serverip]][no_point_domain].has_key(no_point_url):
            ipdict[row[serverip]][no_point_domain][no_point_url] = {}

        # insert url timestamp 
        tree_url = ipdict[row[serverip]][no_point_domain][no_point_url]
        if not tree_url.has_key(timestamp): 
            tree_url[timestamp] = dataset.insert({'time': timestamp})

        # fill data docment record
        fields = {}
        for i in range(34): fields[names[i]] = row[i]

        # insert record to mongodb
        dataset.update({'_id': tree_url[timestamp]}, {"$push": {"records": fields}})
    for i in ipdict:
        ipset.insert(ipdict[i])
insert()

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

ipdict={};
datadict={};
sql = "SELECT * FROM http_log_000000004 limit 0, 1000000"
#sql = "SELECT * FROM http_log_000000004 where server = '192.168.1.12' limit 0, 10"
mysqldb = MySQLdb.connect("localhost","root","123456","ipm" )
cursor = mysqldb.cursor()

def ip_url():
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        no_point_domain = row[server].replace('.', '_')
        if not ipdict.has_key(row[serverip]):
            ipdict[row[serverip]] = {}
            ipdict[row[serverip]]['_id'] = row[serverip]
        if not ipdict[row[serverip]].has_key(no_point_domain):
            ipdict[row[serverip]][no_point_domain] = {}
    
        no_point_url = row[url].replace('.', '_')
        if not ipdict[row[serverip]][no_point_domain].has_key(no_point_url):
            ipdict[row[serverip]][no_point_domain][no_point_url] = {}
            ipdict[row[serverip]][no_point_domain][no_point_url]['appllatency'] = row[appllatency]
            ipdict[row[serverip]][no_point_domain][no_point_url]['clientlatency'] = row[clientlatency]
            ipdict[row[serverip]][no_point_domain][no_point_url]['serverlatency'] = row[serverlatency]
            ipdict[row[serverip]][no_point_domain][no_point_url]['rtt'] = row[rtt]
    for i in ipdict:
        #print json.dumps(ipdict[i], indent=2)
        coll.insert(ipdict[i])
    print len(ipdict)

def del_curr_data(_dict):
    if not _dict or type(_dict).__name__ != 'dict':
        return
    for d,x in _dict.items():
        if d == "curr_data":
            del _dict[d]
            return
        else:
            del_curr_data(_dict[d])

def ip_only():
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        time = str(int(row[begintime]))
        no_point_url = row[url].replace('.', '_')
        if -1 != no_point_url.find('?'): no_point_url = no_point_url[:no_point_url.find('?')]
        no_point_domain = row[server].replace('.', '_')

        # build index collection
        if not ipdict.has_key(row[serverip]):
            ipdict[row[serverip]] = {}
            ipdict[row[serverip]]['_id'] = row[serverip]
        if not ipdict[row[serverip]].has_key(no_point_domain):
            ipdict[row[serverip]][no_point_domain] = {}
        if not ipdict[row[serverip]][no_point_domain].has_key(no_point_url):
            ipdict[row[serverip]][no_point_domain][no_point_url] = {}
        tree_url = ipdict[row[serverip]][no_point_domain][no_point_url]

        # 15000 is the max doc bson size 
        if not tree_url.has_key('curr_data') or len(tree_url['curr_data']) > 15000:
            insert_id = str(dataset.insert({'time': time}))
            dataset.remove({'time': time})
            tree_url[time] = insert_id
            datadict[insert_id] = {}
            datadict[insert_id]['time'] = time
            datadict[insert_id]['_id'] = insert_id
            tree_url['curr_data'] = datadict[insert_id]['data'] = []

        # fill record fields
        fields = {}
        for i in range(34): fields[names[i]] = row[i]
        tree_url['curr_data'].append(fields)
    print "=============fetch mysql end==================="

    for i in datadict:
        print len(datadict[i]['data'])
        dataset.insert(datadict[i])
#        print json.dumps(datadict[i], indent=2)
    del_curr_data(ipdict)
    for i in ipdict:
        ipset.insert(ipdict[i])
#        print json.dumps(ipdict[i], indent=2)

def fetch(addr):
    ip = ipset.find_one({"_id": addr})
    datakeys = []
    for k, v in ip.items():
        if k == "_id": continue
        for _k, _v in v.items():
            for __k, __v in _v.items():
                datakeys.append(__v)
    fetch_num = 0
    for i in datakeys:
        record = dataset.find_one({"_id" : i})
#        print json.dumps(record['data'], indent=2)
        fetch_num += len(record['data'])
    print fetch_num

def fetch_mysql(addr):
    cursor.execute("select * from http_log_000000004 where serverip=" + str(addr))
    results = cursor.fetchall()
    return len(results)

ip_only()
#for ip in ips:
#    starttime1 = time.clock()  
#    fetch(ip)
#    endtime1 = time.clock()  
#    print endtime1 - starttime1

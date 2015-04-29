#!/usr/bin/python

import json
import time
import pymongo
import datetime

_id, client, server, protocol, method, url, httpreturncode, location, referer, useragent, contenttype, _bytes, begintime, endtimewithpayload, flowhash, cookie, terminator, appllatency, clientlatency, serverlatency, rtt, applicationid, application, balancerhost, serverip, rehttppkts, client2server_teid, server2client_teid, flowusername, fbookchat, port, loc_id, loc_id_cn, forward = range(34)
names = ["_id", "client", "server", "protocol", "method", "url", "httpreturncode", "location", "referer", "useragent", "contenttype", "_bytes", "begintime", "endtimewithpayload", "flowhash", "cookie", "terminator", "appllatency", "clientlatency", "serverlatency", "rtt", "applicationid", "application", "balancerhost", "serverip", "rehttppkts", "client2server_teid", "server2client_teid", "flowusername", "fbookchat", "port", "loc_id", "loc_id_cn", "forward"]
ips = [3720385042, 3396081437, 1875813415, 2099758888, 2099713289, 1696398356, 2008822372, 3232235788, 2071819107, 2067447099, 3396084028, 2071797212, 2099708298, 3395987888]

conn = pymongo.Connection()
mongodb = conn.test
ipset = mongodb.ipset
dataset = mongodb.dataset

def fetch(addr):
    ip = ipset.find_one({"_id": addr})
    datakeys = []
    for k, v in ip.items():
        if k == "_id": continue
        for _k, _v in v.items():
            for __k, __v in _v.items():
                if __k == "curr": continue
                datakeys.append(__v['key'])
    fetch_num = 0
    for i in datakeys:
        record = dataset.find_one({"_id" : i})
#        print json.dumps(record['data'], indent=2)
        fetch_num += len(record['records'])
    print len(datakeys), fetch_num

for ip in ips:
    starttime1 = time.clock()  
    fetch(ip)
    endtime1 = time.clock()  
    print endtime1 - starttime1

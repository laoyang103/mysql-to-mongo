#!/usr/bin/python

import json
import time
import pymongo
import datetime

_id, client, server, protocol, method, url, httpreturncode, location, referer, useragent, contenttype, _bytes, begintime, endtimewithpayload, flowhash, cookie, terminator, appllatency, clientlatency, serverlatency, rtt, applicationid, application, balancerhost, serverip, rehttppkts, client2server_teid, server2client_teid, flowusername, fbookchat, port, loc_id, loc_id_cn, forward = range(34)
names = ["_id", "client", "server", "protocol", "method", "url", "httpreturncode", "location", "referer", "useragent", "contenttype", "_bytes", "begintime", "endtimewithpayload", "flowhash", "cookie", "terminator", "appllatency", "clientlatency", "serverlatency", "rtt", "applicationid", "application", "balancerhost", "serverip", "rehttppkts", "client2server_teid", "server2client_teid", "flowusername", "fbookchat", "port", "loc_id", "loc_id_cn", "forward"]
ips = [714914305, 714914306, 3720384515, 714920453, 714918407, 714918409, 2071867915, 3720385037, 2071813134, 3232235779, 1696398356, 978572313, 1875813405, 2067444743, 1032313388, 3720385042, 1875813412, 1875813415, 1007025710, 1875790897, 2067443775, 1008526400, 1872839747, 1008521814, 2008822372, 2099769445, 2008822382, 3733195395, 1032296897, 2099765394, 1885336235, 1008591533, 1875826869, 3720488132, 2071871186, 1008530643, 2071871188, 2067443936, 3702869744, 2099758888, 2071816437, 3702869754, 2071866107, 1008530686, 1875830529, 712550659, 1875814660, 2008845061, 2099713289, 2099713290, 3232235788, 1027051278, 1884967703, 2067447066, 712531713, 3396081437, 2059521823, 1861867298, 3526677796, 2071794469, 1919966504, 1919966510, 3068961588, 2067447098, 2067447099, 3396084028, 2071815490, 1885002055, 3720384519, 2071819107, 3396085094, 3029703534, 1884983159, 2099708298, 1032305036, 1032321935, 3396139760, 2071819172, 3396139761, 1850426695, 3395987888, 3396077498, 2071819196, 2071797185, 3396077510, 2100179400, 1032322507, 1032322508, 2071797212, 3396139770, 2086640606, 3396139771, 714920451, 2071797244, 1850441706, 3702876656, 2071797235, 2007483386, 3726611452]

conn = pymongo.Connection()
mongodb = conn.test
ipset = mongodb.ipset
dataset = mongodb.dataset

def fetch(addr):
    starttime1 = time.clock()  
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
    endtime1 = time.clock()  
    print len(datakeys), fetch_num, endtime1 - starttime1

def fetch_count():
    fetch_num = 0
    ip = dataset.find()
    for i in ip:
        fetch_num += len(i['records'])
    print fetch_num


for ip in ips:
    fetch(ip)

fetch_count()

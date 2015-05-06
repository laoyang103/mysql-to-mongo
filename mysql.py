#!/usr/bin/python

import json
import time
import random
import datetime
import MySQLdb

_id, client, server, protocol, method, url, httpreturncode, location, referer, useragent, contenttype, _bytes, begintime, endtimewithpayload, flowhash, cookie, terminator, appllatency, clientlatency, serverlatency, rtt, applicationid, application, balancerhost, serverip, rehttppkts, client2server_teid, server2client_teid, flowusername, fbookchat, port, loc_id, loc_id_cn, forward = range(34)
names = ["_id", "client", "server", "protocol", "method", "url", "httpreturncode", "location", "referer", "useragent", "contenttype", "_bytes", "begintime", "endtimewithpayload", "flowhash", "cookie", "terminator", "appllatency", "clientlatency", "serverlatency", "rtt", "applicationid", "application", "balancerhost", "serverip", "rehttppkts", "client2server_teid", "server2client_teid", "flowusername", "fbookchat", "port", "loc_id", "loc_id_cn", "forward"]
ips = [3720385042, 3396081437, 1875813415, 2099758888, 2099713289, 1696398356, 2008822372, 3232235788, 2071819107, 2067447099, 3396084028, 2071797212, 2099708298, 3395987888]

sql = "SELECT * FROM http_log_000000004 limit 0, 1000000"
#sql = "SELECT * FROM http_log_000000004 where server = '192.168.1.12' limit 0, 10"
mysqldb = MySQLdb.connect("localhost","root","123456","ipm" )

def fetch_mysql():
    for num in range(10, 40):
        cursor = mysqldb.cursor()
        cursor.execute("select avg(case when ISNULL(loadpage) then 0 else loadpage end) loadpage, \
                sum(traffic) traffic from (select count(1) traffic, floor(avg(endtimewithpayload * 1000 - begintime * 1000)) \
                    loadpage from http_log_0000000" + str(num) +" where begintime >= 1400881080 and begintime < 1450881140 and  \
                    (`server` = 'toruk.tanx.com" + str(random.randint(12, 200)) + "' and (url like '/ex?%' or url = '/ex'))) http_log")
        results = cursor.fetchall()
        print results

fetch_mysql()
#for ip in ips:
#    starttime1 = time.clock()  
#    fetch(ip)
#    endtime1 = time.clock()  
#    print endtime1 - starttime1

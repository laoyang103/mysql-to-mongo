#!/bin/bash

NUM_MESSAGE=10
for ((i = 0; i < $NUM_MESSAGE; i++)); do
    int_rnd=`expr $RANDOM % 10000`
    server_rnd=`date +%s%N | md5sum | head -c 15`
    url_rnd=`date +%s%N | md5sum | head -c 15`

    server_rnd=`echo 12`
    url_rnd=`echo alert`

    # java -cp mongo-java-driver-2.13.1.jar: a serverip 3232235788 \| id \$gt $int_rnd \| url \$regex $url_rnd server \$regex $server_rnd
    # java -cp mysql-connector-java-5.1.10-bin.jar: b "select count(id) from http_log_000000004 where  serverip=3232235788 and id > $int_rnd and url like '%$url_rnd%' and server like '%$server_rnd%'"

    java -cp mongo-java-driver-2.13.1.jar: a \| id \$gt $int_rnd \| url \$regex $url_rnd server \$regex $server_rnd
    java -cp mysql-connector-java-5.1.10-bin.jar: b "select count(id) from http_log_000000004 where  id > $int_rnd and url like '%$url_rnd%' and server like '%$server_rnd%'"
done

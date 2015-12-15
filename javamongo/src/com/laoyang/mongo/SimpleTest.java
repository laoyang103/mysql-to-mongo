package com.laoyang.mongo;  

import java.util.List;
import java.util.Iterator;
import com.mongodb.*;
import com.mongodb.util.*;
import org.jongo.Jongo;
import org.jongo.MongoCursor;
import org.jongo.MongoCollection;
import com.laoyang.mongo.Communication;

public class SimpleTest {

    public static void main(String[] args) {
        try {
            Communication ist = null;
            MongoClientURI uri  = new MongoClientURI("mongodb://localhost/ipm"); 
            MongoClient client = new MongoClient(uri);
            Jongo jongo = new Jongo(client.getDB(uri.getDatabase()));
            MongoCollection coll = jongo.getCollection("3001.3232235884.5000"); 

            System.out.println(System.currentTimeMillis());

            List<Communication> all = coll.
                aggregate("{$match: {'start': 1449298116}}").
                and("{$unwind: '$data'}").
                and("{$match: {'data.starttime': {$gt: 1449298186}}}").
                and("{$project: {'starttime': '$data.starttime', 'endtime': '$data.endtime', 'srcip': '$data.srcip', 'srcport': '$data.srcport', 'proto': '$data.proto', 'srcsent': '$data.srcsent', 'dstsent': '$data.dstsent'}}").
                as(Communication.class);
            for (Communication comm: all) {
                System.out.println(comm);
            }

            System.out.println(System.currentTimeMillis());

            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
// {"start": {"$lt": "1"}, "data": {"$elemMatch": {"starttime": "1449298156"}}}
// db.getCollection("3001.3232235884.5000").aggregate([{"$match": {"start": "0", "data.starttime": {"$gt": "1449298154"}}}, {$unwind: "$data"}]).pretty()

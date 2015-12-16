package com.laoyang.mongo;  

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import com.mongodb.*;
import com.mongodb.util.*;
import org.jongo.Jongo;
import org.jongo.MongoCursor;
import org.jongo.MongoCollection;
import com.laoyang.mongo.DocKey;
import com.laoyang.mongo.Communication;

public class SimpleTest {

    private static Jongo jongo = null;
    private static MongoClientURI uri = null;
    private static MongoClient client = null;

    static {
        try {
            uri = new MongoClientURI("mongodb://localhost/ipml4"); 
            client = new MongoClient(uri);
            jongo = new Jongo(client.getDB(uri.getDatabase()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long start = Long.parseLong(args[1]);
        long end   = Long.parseLong(args[2]);
        List<Communication> all = getAll(args[0], start, end);
        for (Communication comm: all) { 
            System.out.println(comm);
        }
        client.close();
    }

    public static List<Communication> getAll(String busiKey, long starttime, long endtime) {
        List<Communication> retList = new ArrayList<Communication>();
        try {
            MongoCollection coll = jongo.getCollection(busiKey); 
            List<DocKey> keyList = coll.aggregate("{$match: {'start': {$gte: #, $lte: #}}}", starttime, endtime).
                and("{$project: {'start': 1}}").as(DocKey.class);
            for (DocKey dk: keyList) {
                List<Communication> tmp = coll.
                    aggregate("{$match: {'start': #}}", dk.getKey()).
                    and("{$unwind: '$data'}").
                    and("{$match: {'data.starttime': {$gte: #, $lte: #}}}", starttime, endtime).
                    and("{$project: {'starttime': '$data.starttime', 'endtime': '$data.endtime', 'srcip': '$data.srcip', 'srcport': '$data.srcport', 'proto': '$data.proto', 'srcsent': '$data.srcsent', 'dstsent': '$data.dstsent'}}").
                    as(Communication.class);
                retList.addAll(tmp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retList;
    }
}
// System.out.println(System.currentTimeMillis());
// {"start": {"$lt": "1"}, "data": {"$elemMatch": {"starttime": "1449298156"}}}
// db.getCollection("3001.3232235884.5000").aggregate([{"$match": {"start": "0", "data.starttime": {"$gt": "1449298154"}}}, {$unwind: "$data"}]).pretty()
// db.getCollection("0.1249738951.443").find({}, {"start": 1}).sort({"start": -1}).pretty()

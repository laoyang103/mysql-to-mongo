import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
  * Java + MongoDB Hello world Example
  * 
  */
public class a {
    public static void main(String[] args) {
        try {
	    int i = 0;
	    long start;
            //实例化Mongo对象，连接27017端口
            Mongo mongo = new Mongo("localhost", 27017);
            //连接名为yourdb的数据库，假如数据库不存在的话，mongodb会自动建立
            DB db = mongo.getDB("test");
            // Get collection from MongoDB, database named "yourDB"
            //从Mongodb中获得名为yourColleection的数据集合，如果该数据集合不存在，Mongodb会为其新建立
            DBCollection collection = db.getCollection("bigdata");
            // 创建要查询的document
            BasicDBObject searchQuery  = new BasicDBObject();

	    for (; i < args.length; i+=2) {
	        if (args[i].equals("|")) break;
		searchQuery.put(args[i], Long.parseLong(args[i+1]));
	    }
	    i++;
	    for (; i < args.length; i+=3) {
	        if (args[i].equals("|")) break;
		BasicDBObject searchQuery1 = new BasicDBObject();
		searchQuery1.put(args[i+1], Long.parseLong(args[i+2]));
		searchQuery.put(args[i], searchQuery1);
	    }
	    i++;
	    for (; i < args.length; i+=3) {
	        if (args[i].equals("|")) break;
		BasicDBObject searchQuery1 = new BasicDBObject();
		searchQuery1.put(args[i+1], args[i+2]);
		searchQuery.put(args[i], searchQuery1);
	    }

	    System.out.print(searchQuery + ",");

	    //要查的哪些字段 
	    DBObject field = new BasicDBObject();
	    field.put("id", true);

	    // start = System.currentTimeMillis();
            // DBCursor cursor = collection.find(searchQuery, field);
            // while (cursor.hasNext()) {
	    //         cursor.next();
            // }
	    // System.out.println(System.currentTimeMillis() - start);

	    start = System.currentTimeMillis();
	    collection.find(searchQuery, field).count();
	    System.out.println(System.currentTimeMillis() - start);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }
}

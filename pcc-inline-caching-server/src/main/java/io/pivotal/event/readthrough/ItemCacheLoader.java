package io.pivotal.event.readthrough;

import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class ItemCacheLoader implements CacheLoader {
	
	private static LogWriter log;
	
	private DBCollection itemCollection;

	static {
		log = CacheFactory.getAnyInstance().getDistributedSystem().getLogWriter();
	}
	
	public void init(Properties props) {
		String mongoAddress = props.getProperty("mongoAddress");
		String mongoPort = props.getProperty("mongoPort");
		String dbName = props.getProperty("dbName");
		String collectionName = props.getProperty("collectionName");
		
		itemCollection = new MongoClient(mongoAddress, Integer.parseInt(mongoPort)).getDB(dbName).getCollection(collectionName);
	}

	public PdxInstance load(LoaderHelper helper) {
		log.info("Cache miss... Loading data from MongoDB...");
		
        String itemId = (String) helper.getKey();
        BasicDBObject query = new BasicDBObject("reference", itemId);

		DBCursor cursor = itemCollection.find(query);
        
		if (cursor.hasNext()) {
			DBObject doc = cursor.next();
			return JSONFormatter.fromJSON(doc.toString());
		} else {
			return null;
		}
    }

	@Override
	public void close() {}
}

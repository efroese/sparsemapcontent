package org.sakaiproject.nakamura.lite.storage.mongo;

import java.util.Map;

import com.mongodb.DBObject;

public class MongoClientUtils {
	
	public static void copyToDBObject(DBObject dbo, Map<String,Object> properties){
		for (String key: properties.keySet()) {
			dbo.put(key, properties.get(key));
		}
	}

}

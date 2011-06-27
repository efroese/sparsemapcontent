package org.sakaiproject.nakamura.lite.storage.mongo;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class MongoUtils {

	/**
	 * Convert a {@link DBObject} from into something that the rest of sparse can work with.
	 * @param dbo the object fetched from the DB.
	 * @return the dbo as a Map.
	 */
	public static Map<String,Object> convertDBObjectToMap(DBObject dbo){
		Map<String,Object> map = null; 
		if (dbo != null){
			map = new HashMap<String,Object>();
			for (String key: dbo.keySet()){
				Object val = dbo.get(key);
				// The rest of sparsemapcontent expects Arrays.
				// Mongo returns {@link BasicDBList}s no matter what.
				if (val instanceof BasicDBList){
					BasicDBList dbl = (BasicDBList) val;
					// Not really happy about using a String here 
					// but it makes more tests pass in the ContentManagerFinderImplMan case.
					map.put(key, dbl.toArray(new String[0]));
				}
				else {
					map.put(key, val);
				}
			}
		}
		return map;
	}
}

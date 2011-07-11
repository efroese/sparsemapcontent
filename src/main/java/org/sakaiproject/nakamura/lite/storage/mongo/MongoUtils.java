package org.sakaiproject.nakamura.lite.storage.mongo;

import java.util.HashMap;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.RemoveProperty;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoUtils {

	/**
	 * Take the properties as given by sparsemap and modify them for insertion into mongo.
	 * @param props the properties of this content
	 * @return the properties ready for Mongo
	 */
	public static DBObject cleanPropertiesForInsert(Map<String, Object> props) {
		DBObject cleaned = new BasicDBObject();
		DBObject removeFields = new BasicDBObject();
		DBObject setFields = new BasicDBObject();
		for(String key : props.keySet()){
			Object value = props.get(key);
			// Replace the sparse RemoveProperty with the Mongo $unset.
			if (value instanceof RemoveProperty){
				removeFields.put(key, 1);
			}
			else if (value != null){
				setFields.put(key, value);
			}
		}
		// Cannot do a $set modifier on the _id field
		if (setFields.containsField("_id")){
			setFields.removeField("_id");
		}
		if (setFields.keySet().size() > 0){
			cleaned.put(Operators.SET, setFields);
		}
		if (removeFields.keySet().size() > 0){
			cleaned.put(Operators.UNSET, removeFields);
		}
		return cleaned;
	}

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
					// Not really happy about using a String[] here
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

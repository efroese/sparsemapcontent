package org.sakaiproject.nakamura.lite.storage.mongo;

import java.util.HashMap;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.lite.content.InternalContent;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoUtils {

	public static String MONGO_FIELD_DOT_REPLACEMENT = "&#46;";

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
			key = escapeFieldName(key);
			// Replace the sparse RemoveProperty with the Mongo $unset.
			if (value instanceof RemoveProperty){
				removeFields.put(key, 1);
			}
			else if (value != null){
				setFields.put(key, value);
			}
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
	@SuppressWarnings("deprecation")
	public static Map<String,Object> convertDBObjectToMap(DBObject dbo){
		Map<String,Object> map = null;
		if (dbo != null){
			map = new HashMap<String,Object>();
			for (String key: dbo.keySet()){
				Object val = dbo.get(key);
				key = unescapeFieldName(key);
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

			// Delete the Mongo-supplied internal _id
			if (map.containsKey(MongoClient.MONGO_INTERNAL_ID_FIELD)){
				map.remove(MongoClient.MONGO_INTERNAL_ID_FIELD);
			}
			// Rename the sparse id property to InternalContent.getUuidField() so the rest of sparse can use that property nameield.
			if (map.containsKey(MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD)){
				map.put(InternalContent.getUuidField(), map.get(MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD));
				map.remove(MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD);
			}
		}
		return map;
	}

	public static String escapeFieldName(String key) {
		return key.replaceAll("\\.", MONGO_FIELD_DOT_REPLACEMENT);
	}

	public static String unescapeFieldName(String key) {
		return key.replaceAll(MONGO_FIELD_DOT_REPLACEMENT, ".");
	}
}

package org.sakaiproject.nakamura.lite.storage.mongo;

import java.util.HashMap;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.lite.content.InternalContent;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoUtils {

	/*
	 * MongoDB does not allow . and $ in a field name.
	 * http://www.mongodb.org/display/DOCS/Legal+Key+Names
	 *
	 * Noone should be able to type this into the UX and hopefully the UX devs are
	 * not going to pick this as a chaaratcer in their field names not allow users to
	 * create arbitrary fields.
	 *
	 */
	public static String MONGO_FIELD_DOT_REPLACEMENT = "�";
	public static String MONGO_FIELD_DOLLAR_REPLACEMENT = "�";

	/**
	 * Take the properties as given by sparsemap and modify them for insertion into mongo.
	 * @param props the properties of this content
	 * @return the properties ready for Mongo
	 */
	public static DBObject cleanPropertiesForInsert(Map<String, Object> props) {
		DBObject cleaned = new BasicDBObject();
		DBObject removeFields = new BasicDBObject();
		DBObject updatedFields = new BasicDBObject();

		// Partition the properties into update and remove ops
		for(String key : props.keySet()){
			Object value = props.get(key);
			key = escapeFieldName(key);
			// Replace the sparse RemoveProperty with the Mongo $unset.
			if (value instanceof RemoveProperty){
				removeFields.put(key, 1);
			}
			else if (value != null){
				updatedFields.put(key, value);
			}
		}
		if (updatedFields.keySet().size() > 0){
			cleaned.put(Operators.SET, updatedFields);
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

	/**
	 * Create a key that's safe to use as a field name in MongoDB
	 * @param key the SMC property key
	 * @return the MongoDB field name
	 */
	public static String escapeFieldName(String key) {
		if (key == null){
			return null;
		}
		key = key.replaceAll("\\.", MONGO_FIELD_DOT_REPLACEMENT);
		key = key.replaceAll("\\$", MONGO_FIELD_DOLLAR_REPLACEMENT);
		return key;
	}

	/**
	 * Transform the MongoDB document field name into a SMC property key.
	 * @param fieldName the name of the field in the MongoDB document.
	 * @return the property key in SMC
	 */
	public static String unescapeFieldName(String fieldName) {
		if (fieldName == null){
			return null;
		}
		fieldName = fieldName.replaceAll(MONGO_FIELD_DOT_REPLACEMENT, ".");
		fieldName = fieldName.replaceAll(MONGO_FIELD_DOLLAR_REPLACEMENT, "\\$");
		return fieldName;
	}
}

package org.sakaiproject.nakamura.lite.storage.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.StorageClient;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class MongoClient implements StorageClient {

	private DB mongodb;
	
	public MongoClient(DB mongodb) {
		this.mongodb = mongodb;
		this.mongodb.authenticate("admin", "admin".toCharArray());
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> get(String keySpace, String columnFamily,
			String key) throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject();
		query.put("_id", key);
		DBCursor cursor = collection.find(query);
		Map<String,Object> result = null; 
		if (cursor.size() == 1){
			result = (Map<String,Object>)cursor.next();
		}
		return result;
	}

	public void insert(String keySpace, String columnFamily, String key,
			Map<String, Object> values, boolean probablyNew)
			throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject toInsert = new BasicDBObject();
		MongoClientUtils.copyToDBObject(toInsert, values);
		collection.insert(toInsert);
	}

	public void remove(String keySpace, String columnFamily, String key)
			throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject();
		query.put("_id", key);
		collection.remove(query);
	}

	public InputStream streamBodyOut(String keySpace, String columnFamily,
			String contentId, String contentBlockId, String streamId,
			Map<String, Object> content) throws StorageClientException,
			AccessDeniedException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<String, Object> streamBodyIn(String keySpace,
			String columnFamily, String contentId, String contentBlockId,
			String streamId, Map<String, Object> content, InputStream in)
			throws StorageClientException, AccessDeniedException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	public Iterator<Map<String, Object>> find(String keySpace,
			String columnFamily, Map<String, Object> properties)
			throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject();
	
		MongoClientUtils.copyToDBObject(query, properties);
		DBCursor cursor = collection.find(query);
		Iterator<?> itr = (Iterator<?>)cursor.iterator();
		return (Iterator<Map<String, Object>>) itr;
	}

	public void close() {
		// TODO Auto-generated method stub
	}

	public DisposableIterator<Map<String, Object>> listChildren(
			String keySpace, String columnFamily, String key)
			throws StorageClientException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean hasBody(Map<String, Object> content, String streamId) {
		// TODO Auto-generated method stub
		return false;
	}
}

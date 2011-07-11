package org.sakaiproject.nakamura.lite.storage.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.lite.content.FileStreamContentHelper;
import org.sakaiproject.nakamura.lite.content.InternalContent;
import org.sakaiproject.nakamura.lite.content.StreamedContentHelper;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.RowHasher;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoClient implements StorageClient, RowHasher {

	private static final Logger log = LoggerFactory.getLogger(MongoClient.class);

	private DB mongodb;

	// Reads and Writes file content to a filesystem.
	// TODO: Replace this with a GridFS helper.
	private StreamedContentHelper streamedContentHelper;

	public MongoClient(DB mongodb, Map<String,Object> props) {
		log.debug("Created");
		this.mongodb = mongodb;

		String user = StorageClientUtils.getSetting(props.get(MongoClientPool.PROP_MONGO_USER), MongoClientPool.PROP_MONGO_USER);
		String password = StorageClientUtils.getSetting(props.get(MongoClientPool.PROP_MONGO_USER), MongoClientPool.PROP_MONGO_USER);
		if (!"".equals(user) && !"".equals(password)){
			this.mongodb.authenticate("admin", "admin".toCharArray());
		}
		this.mongodb.requestStart();

		this.streamedContentHelper = new FileStreamContentHelper(this, props);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#get(java.lang.String, java.lang.String, java.lang.String)
	 */
	public Map<String, Object> get(String keySpace, String columnFamily,
			String key) throws StorageClientException {
		log.info("get {}:{}:{}", new Object[]{keySpace, columnFamily, key});
		DBCollection collection = mongodb.getCollection(columnFamily);

		// Pretty straightforward. Just query by the id.
		BasicDBObject query = new BasicDBObject();
		query.put("id", key);
		DBCursor cursor = collection.find(query);
		Map<String,Object> result = null;
		if (cursor.size() == 1){
			result = MongoUtils.convertDBObjectToMap(cursor.next());
		}
		if (result == null){
			result = new HashMap<String, Object>();
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#insert(java.lang.String, java.lang.String, java.lang.String, java.util.Map, boolean)
	 */
	public void insert(String keySpace, String columnFamily, String key,
			Map<String, Object> values, boolean probablyNew)
	throws StorageClientException {
		HashMap<String,Object> mutableValues = new HashMap<String,Object>(values);
		// Set the parent path hash if this is a piece of content
		if (values.keySet().contains(InternalContent.PATH_FIELD)) {
			if ( !StorageClientUtils.isRoot(key)) {
				mutableValues.put(InternalContent.PARENT_HASH_FIELD,
						rowHash(keySpace, columnFamily, StorageClientUtils.getParentObjectPath(key)));
			}
		}

		DBCollection collection = mongodb.getCollection(columnFamily);

		// document to update.
		DBObject query = new BasicDBObject("id", key);

		// The values we're going to put in mongo
		DBObject insert = MongoUtils.cleanPropertiesForInsert(mutableValues);

		// Update or insert a single document.
		collection.update(query, insert, true, false);
		log.info("insert {}:{}:{} => {}", new Object[] {keySpace, columnFamily, key, insert.toString()});
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#remove(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void remove(String keySpace, String columnFamily, String key)
	throws StorageClientException {
		log.info("remove {}:{}:{}", new Object[]{keySpace, columnFamily, key});
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject();
		query.put("id", key);
		collection.remove(query);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#streamBodyOut(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map)
	 */
	public InputStream streamBodyOut(String keySpace, String columnFamily,
			String contentId, String contentBlockId, String streamId,
			Map<String, Object> content) throws StorageClientException,
			AccessDeniedException, IOException {
		return streamedContentHelper.readBody(keySpace, columnFamily, contentBlockId, streamId, content);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#streamBodyIn(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map, java.io.InputStream)
	 */
	public Map<String, Object> streamBodyIn(String keySpace,
			String columnFamily, String contentId, String contentBlockId,
			String streamId, Map<String, Object> content, InputStream in)
			throws StorageClientException, AccessDeniedException, IOException {
		Map<String,Object> meta = streamedContentHelper.writeBody(keySpace, columnFamily, contentId, contentBlockId, streamId, content, in);
		return meta;
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#find(java.lang.String, java.lang.String, java.util.Map)
	 */
	@SuppressWarnings("unchecked")
	public DisposableIterator<Map<String, Object>> find(String keySpace,
			String columnFamily, Map<String, Object> properties)
			throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject(properties);

		// Go through the properties of the query
		for (String key: properties.keySet()){
			Object val = properties.get(key);

			if (val instanceof Map){
				// This is how it comes from sparse
				// properties = { "orset0" : { "fieldName" : [ "searchVal0", "searchVal1" ] } }
				Map<String,Object> multiValQueryMap = (Map<String, Object>) val;
				String field = multiValQueryMap.keySet().iterator().next();
				List<String> searchValues = (List<String>)multiValQueryMap.get(field);

				// This is what mongo expects
				// mongoQuery = { "$or" : [ BasicDBObject("field", "val0"), 
				//                             BasicDBObject("field", "val1") ] } 
				ArrayList<BasicDBObject> mongoQuery = new ArrayList<BasicDBObject>();
				for(String searchVal: searchValues){
					mongoQuery.add(new BasicDBObject(field, searchVal));
				}

				if (key.startsWith("orset")){
					// Remove the original query and add a Mongo OR query.
					query.remove(key);
					query.put(Operators.OR, mongoQuery);
				}
			}
			else if (val instanceof List){
				// What mongo expects
				// { "fieldName" : { "$all" : [ "valueX", "valueY" ] } }
				List<String> valList = (List<String>)val;
				BasicDBObject mongoSet = new BasicDBObject();
				mongoSet.put(Operators.ALL, valList);
				// overwrite the original value of key
				query.put(key, mongoSet);
			}
		}

		// See if we need to sort
		DBCursor cursor = collection.find(query);
		if (properties.containsKey("_sort")){
			query.remove("_sort");
			cursor.sort(new BasicDBObject((String)properties.get("_sort"), 1));
		}

		final Iterator<?> itr = (Iterator<?>)cursor.iterator();
		return new DisposableIterator<Map<String,Object>>() {

			public boolean hasNext() {
				return itr.hasNext();
			}

			public Map<String, Object> next() {
				return MongoUtils.convertDBObjectToMap((DBObject)itr.next());
			}

			public void remove() {
				// TODO Auto-generated method stub
			}

			public void close() {
				// TODO Auto-generated method stub
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#close()
	 */
	public void close() {
		log.debug("Closed");
		this.mongodb.requestDone();
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#listChildren(java.lang.String, java.lang.String, java.lang.String)
	 */
	public DisposableIterator<Map<String, Object>> listChildren(
			String keySpace, String columnFamily, String key)
			throws StorageClientException {
		// this will load all child object directly.
		String hash = rowHash(keySpace, columnFamily, key);
		log.debug("Finding {}:{}:{} as {} ", new Object[]{keySpace,columnFamily, key, hash});
		return find(keySpace, columnFamily, ImmutableMap.of(InternalContent.PARENT_HASH_FIELD, (Object)hash));
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#hasBody(java.util.Map, java.lang.String)
	 */
	public boolean hasBody(Map<String, Object> content, String streamId) {
		return streamedContentHelper.hasStream(content, streamId);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.RowHasher#rowHash(java.lang.String, java.lang.String, java.lang.String)
	 */
	public String rowHash(String keySpace, String columnFamily, String key)
	throws StorageClientException {
		MessageDigest hasher;
		try {
			hasher = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e1) {
			throw new StorageClientException("Unable to get hash algorithm " + e1.getMessage(), e1);
		}
		String keystring = keySpace + ":" + columnFamily + ":" + key;
		byte[] ridkey;
		try {
			ridkey = keystring.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			ridkey = keystring.getBytes();
		}
		String hash = StorageClientUtils.encode(hasher.digest(ridkey)); 
		log.info("{}:{}:{} => {}", new Object[]{keySpace, columnFamily, key, hash});
		return hash;
	}
}

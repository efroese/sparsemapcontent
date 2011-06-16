package org.sakaiproject.nakamura.lite.storage.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.lite.content.FileStreamContentHelper;
import org.sakaiproject.nakamura.lite.content.StreamedContentHelper;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.RowHasher;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class MongoClient implements StorageClient, RowHasher {

	private static final Logger log = LoggerFactory.getLogger(MongoClient.class);

	private DB mongodb;

	// Reads and Writes file content to a filesystem.
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

		this.streamedContentHelper = new FileStreamContentHelper(null, props);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#get(java.lang.String, java.lang.String, java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> get(String keySpace, String columnFamily,
			String key) throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);

		// Pretty straightforward. Just query by the id.
		BasicDBObject query = new BasicDBObject();
		query.put("id", key);
		DBCursor cursor = collection.find(query);
		Map<String,Object> result = null;
		if (cursor.size() == 1){
			result = (Map<String,Object>)cursor.next();
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
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject insert = new BasicDBObject(cleanProperties(values));
		insert.put("id", key);
		BasicDBObject query = new BasicDBObject("id", key);
		collection.update(query, insert, true, false);
		log.info("Inserting into {}:{}:{}", new Object[] {keySpace, columnFamily, key, values.toString()});
	}

	/**
	 * This is a nasty, dirty, evil hack to see some tests work before I go out the door.
	 * @param props
	 * @return the cleaned props
	 */
	private Map<String, Object> cleanProperties(Map<String, Object> props) {
		Builder<String, Object> cleaned = new ImmutableMap.Builder<String, Object>();
		for(String key : props.keySet()){
			Object val = props.get(key);
			if (!(val instanceof RemoveProperty)){
				cleaned.put(key, val);
			}
		}
		return cleaned.build();
	}

	/*
	 * (non-Javadoc)
	 * @see org.sakaiproject.nakamura.lite.storage.StorageClient#remove(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void remove(String keySpace, String columnFamily, String key)
	throws StorageClientException {
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
	public Iterator<Map<String, Object>> find(String keySpace,
			String columnFamily, Map<String, Object> properties)
			throws StorageClientException {
		DBCollection collection = mongodb.getCollection(columnFamily);
		BasicDBObject query = new BasicDBObject(properties);
		DBCursor cursor = collection.find(query);
		Iterator<?> itr = (Iterator<?>)cursor.iterator();
		return (Iterator<Map<String, Object>>) itr;
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
		return null;
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
		return StorageClientUtils.encode(hasher.digest(ridkey));
	}
}

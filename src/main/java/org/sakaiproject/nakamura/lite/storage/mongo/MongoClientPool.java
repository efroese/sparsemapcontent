package org.sakaiproject.nakamura.lite.storage.mongo;

import java.net.UnknownHostException;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.StorageClientPool;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

@Component(immediate = true, metatype = true)
@Service
public class MongoClientPool implements StorageClientPool {
	
	protected Mongo mongo;
	protected DB db;
	
	@Activate
	public void init(Map<String,Object> props) throws MongoException, UnknownHostException {
		mongo = new Mongo(new MongoURI("mongodb://127.0.0.1"));
		db = mongo.getDB("nakamura");
	}

	public StorageClient getClient() throws ClientPoolException {
		return new MongoClient(db);
	}

	public StorageCacheManager getStorageCacheManager() {
		// TODO Auto-generated method stub
		return null;
	}

}

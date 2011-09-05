package org.sakaiproject.nakamura.lite.storage.mongo;

import java.net.UnknownHostException;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.CacheHolder;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.lite.storage.ConcurrentLRUMap;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.StorageClientPool;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

@Component(immediate = true, metatype = true)
@Service
public class MongoClientPool implements StorageClientPool {

	public static final String[] SPARSE_COLLECTION_NAMES = { "au", "ac", "cn" };

	protected Mongo mongo;
	protected DB db;

	private static final String DEFAULT_MONGO_URI = "mongodb://127.0.0.1";
	@Property(value = DEFAULT_MONGO_URI)
	public static final String PROP_MONGO_URI = "mongo.uri";

	private static final String DEFAULT_MONGO_DB = "nakamura";
	@Property(value = DEFAULT_MONGO_DB)
	public static final String PROP_MONGO_DB = "mongo.db";

	private static final String DEFAULT_MONGO_USER = "";
	@Property(value = DEFAULT_MONGO_USER)
	public static final String PROP_MONGO_USER = "mongo.user";

	private static final String DEFAULT_MONGO_PASSWORD = "nakamura";
	@Property(value = DEFAULT_MONGO_PASSWORD)
	public static final String PROP_MONGO_PASSWORD = "mongo.password";

	private static final String DEFAULT_STOREBASE = "store";
	@Property(value = DEFAULT_STOREBASE)
	public static final String PROP_STOREBASE = "mongo.disk.storage.base";

	private static final String[] DEFAULT_INDEXED_KEYS = new String[] {
		"au:" + MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD,
		"au:rep:principalName",
		"au:type",

		"ac:" + MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD,

		"cn:" + MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD,
		"cn:sling:resourceType",
		"cn:sakai:pooled-content-manager",

		"cn:sakai:messagestore",
		"cn:sakai:type",
		"cn:sakai:marker",

		"cn:sakai:tag-uuid",

		"cn:sakai:contactstorepath",
		"cn:sakai:state",
		"cn:firstName",
		"cn:lastName",

		"cn:_created",

		"cn:sakai:category",
		"cn:sakai:messagebox",
		"cn:sakai:from",
		"cn:sakai:subject",
	};
	@Property
	public static final String PROP_INDEXED_COLS = "mongo.indexed.keys";

	@Reference(cardinality=ReferenceCardinality.OPTIONAL_UNARY, policy=ReferencePolicy.DYNAMIC)
	private StorageCacheManager storageManagerCache;

	private Map<String,Object> props;

	private ConcurrentLRUMap<String, CacheHolder> sharedCache;

	private StorageCacheManager defaultStorageManagerCache;

	@Activate
	public void activate(Map<String,Object> props) throws MongoException, UnknownHostException {
		this.mongo = new Mongo(new MongoURI(StorageClientUtils.getSetting(props.get(PROP_MONGO_URI), DEFAULT_MONGO_URI)));
		this.db = mongo.getDB(StorageClientUtils.getSetting(props.get(PROP_MONGO_DB), DEFAULT_MONGO_DB));
		this.props = props;

		this.sharedCache = new ConcurrentLRUMap<String, CacheHolder>(10000);
		// this is a default cache used where none has been provided.
        defaultStorageManagerCache = new StorageCacheManager() {
            public Map<String, CacheHolder> getContentCache() {
                return sharedCache;
            }
            public Map<String, CacheHolder> getAuthorizableCache() {
                return sharedCache;
            }
            public Map<String, CacheHolder> getAccessControlCache() {
                return sharedCache;
            }
        };

		for (String name: SPARSE_COLLECTION_NAMES){
			if (!db.collectionExists(name)){
				db.createCollection(name, null);
				DBCollection collection = db.getCollection(name);
				collection.ensureIndex(new BasicDBObject("id", 1), "id_index", true);
			}
		}

		DBCollection collection;
		String[] keysToIndex = StorageClientUtils.getSetting(props.get(PROP_INDEXED_COLS), DEFAULT_INDEXED_KEYS);
		for (String toIndex: keysToIndex){
			int firstColon = toIndex.indexOf(':');
			String columnFamily = toIndex.substring(0, firstColon);
			String keyName = toIndex.substring(firstColon + 1);
			collection = db.getCollection(columnFamily);
			collection.ensureIndex(keyName);
		}
	}

	public StorageClient getClient() throws ClientPoolException {
		return new MongoClient(db, props);
	}

	public StorageCacheManager getStorageCacheManager() {
        if ( storageManagerCache != null ) {
            if ( sharedCache.size() > 0 ) {
                sharedCache.clear(); // dump any memory consumed by the default cache.
            }
            return storageManagerCache;
        }
        return defaultStorageManagerCache;
    }

}
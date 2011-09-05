package org.sakaiproject.nakamura.lite.storage.mongo;

import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
		"au:rep:principalName",
		"au:type",
		"au:_:parenthash",

		"ac:_:parenthash",

		"cn:_created",
		"cn:_:parenthash",
		"cn:firstName",
		"cn:lastName",

		"cn:sakai:category",
		"cn:sakai:contactstorepath",
		"cn:sakai:from",
		"cn:sakai:marker",
		"cn:sakai:messagebox",
		"cn:sakai:messagestore",
		"cn:sakai:pooled-content-manager",
		"cn:sling:resourceType",
		"cn:sakai:state",
		"cn:sakai:subject",
		"cn:sakai:tag-uuid",
		"cn:sakai:type",

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
				DBCollection collection = db.createCollection(name, null);
				collection.ensureIndex(new BasicDBObject(MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD, 1),
						MongoClient.MONGO_INTERNAL_SPARSE_UUID_FIELD + "_index", true);
			}
		}

		DBCollection collection;
		String[] keysToIndex = StorageClientUtils.getSetting(props.get(PROP_INDEXED_COLS), DEFAULT_INDEXED_KEYS);
		for (String toIndex: keysToIndex){
			String columnFamily = StringUtils.substringBefore(toIndex, ":");
			String keyName = StringUtils.substringAfter(toIndex, ":");
			collection = db.getCollection(columnFamily);
			collection.ensureIndex(new BasicDBObject(keyName, 1), keyName + "_index", false);
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
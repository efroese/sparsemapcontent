/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.lite;


import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.osgi.framework.ServiceReference;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StoreListener;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManagerPlugin;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManagerPluginFactory;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.lite.accesscontrol.AuthenticatorImpl;
import org.sakaiproject.nakamura.lite.authorizable.AuthorizableActivator;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.StorageClientPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component(immediate = true, metatype = true)
@Service(value = Repository.class)
public class RepositoryImpl implements Repository {

    @Reference
    protected Configuration configuration;

    @Reference
    protected StorageClientPool clientPool;
    
    @Reference 
    protected StoreListener storeListener;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            referenceInterface = AuthorizableManagerPluginFactory.class,
            policy = ReferencePolicy.DYNAMIC,
            bind = "addAuthorizableManagerPluginFactory",
            unbind = "removeAuthorizableManagerPluginFactory")
    protected Collection<AuthorizableManagerPluginFactory> authorizableManagerPluginFactories = 
            new CopyOnWriteArrayList<AuthorizableManagerPluginFactory>();

    public RepositoryImpl() {
    }

    @Activate
    public void activate(Map<String, Object> properties) throws ClientPoolException,
            StorageClientException, AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthorizableActivator authorizableActivator = new AuthorizableActivator(client,
                    configuration);
            authorizableActivator.setup();
        } finally {
            client.close();
            clientPool.getClient();
        }
    }

    @Deactivate
    public void deactivate(Map<String, Object> properties) throws ClientPoolException {
    }

    public Session login(String username, String password) throws ClientPoolException,
            StorageClientException, AccessDeniedException {
        return openSession(username, password);
    }

    public Session login() throws ClientPoolException, StorageClientException,
            AccessDeniedException {
        return openSession(User.ANON_USER);
    }

    public Session loginAdministrative() throws ClientPoolException, StorageClientException,
            AccessDeniedException {
        return openSession(User.ADMIN_USER);
    }

    public Session loginAdministrative(String username) throws StorageClientException,
            ClientPoolException, AccessDeniedException {
        return openSession(username);
    }

	private Session openSession(String username, String password) throws StorageClientException,
            AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl(client, configuration);
            User currentUser = authenticatorImpl.authenticate(username, password);
            if (currentUser == null) {
                throw new StorageClientException("User " + username + " cant login with password");
            }
            return new SessionImpl(this, currentUser, client, configuration, clientPool.getStorageCacheManager(), storeListener, getAuthorizableManagerPlugins());
        } catch (ClientPoolException e) {
            clientPool.getClient();
            throw e;
        } catch (StorageClientException e) {
            clientPool.getClient();
            throw e;
        } catch (AccessDeniedException e) {
            clientPool.getClient();
            throw e;
        } catch (Throwable e) {
            clientPool.getClient();
            throw new StorageClientException(e.getMessage(), e);
        }
    }

	private Session openSession(String username) throws StorageClientException,
            AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl(client, configuration);
            User currentUser = authenticatorImpl.systemAuthenticate(username);
            if (currentUser == null) {
                throw new StorageClientException("User " + username
                        + " does not exist, cant login administratively as this user");
            }
            return new SessionImpl(this, currentUser, client, configuration,
                        clientPool.getStorageCacheManager(), storeListener, getAuthorizableManagerPlugins());
        } catch (ClientPoolException e) {
            clientPool.getClient();
            throw e;
        } catch (StorageClientException e) {
            clientPool.getClient();
            throw e;
        } catch (AccessDeniedException e) {
            clientPool.getClient();
            throw e;
        } catch (Throwable e) {
            clientPool.getClient();
            throw new StorageClientException(e.getMessage(), e);
        }
    }

	/**
	 * Create a {@link Collection} of {@link AuthorizableManagerPlugin}s by iterating
	 * over the list of {@link AuthorizableManagerPluginFactory} objects registered
	 * as Services as calling {@link AuthorizableManagerPluginFactory#getAuthorizableManagerPlugin()}
	 * @return the list of plugins for the AuthorizableManager
	 */
	private Collection<AuthorizableManagerPlugin> getAuthorizableManagerPlugins() {
		Collection<AuthorizableManagerPlugin> amps = null;
		if (authorizableManagerPluginFactories != null){
			amps = new ArrayList<AuthorizableManagerPlugin>();
			for (AuthorizableManagerPluginFactory ampf : authorizableManagerPluginFactories ){
				AuthorizableManagerPlugin amp = ampf.getAuthorizableManagerPlugin();
				if (amp != null) {
					amps.add(amp);
				}
			}
		}
		return amps;
	}

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConnectionPool(StorageClientPool connectionPool) {
        this.clientPool = connectionPool;
    }

    public void setStorageListener(StoreListener storeListener) {
        this.storeListener = storeListener;
    }

    /**
     * Used by OSGI to add a {@link ServiceReference} for an {@link AuthorizableManagerPluginFactory}
     * when a new {@link AuthorizableManagerPluginFactory} {@link Service} is registered. 
     * @param ampf the {@link AuthorizableManagerPluginFactory} to be added.
     */
    public void addAuthorizableManagerPluginFactory(AuthorizableManagerPluginFactory ampf){
        authorizableManagerPluginFactories.add(ampf);
    }

    /**
     * Used by OSGI to remove a {@link ServiceReference} for an {@link AuthorizableManagerPluginFactory}
     * when a {@link AuthorizableManagerPluginFactory} {@link Service} is unregistered. 
     * @param ampf the {@link AuthorizableManagerPluginFactory} to be removed.
     */
    public void removeAuthorizableManagerPluginFactory(AuthorizableManagerPluginFactory ampf){
        authorizableManagerPluginFactories.remove(ampf);
    }

}

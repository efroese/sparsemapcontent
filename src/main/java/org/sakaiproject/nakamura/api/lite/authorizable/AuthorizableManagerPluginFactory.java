package org.sakaiproject.nakamura.api.lite.authorizable;

import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.Session;

/**
 * Create an {@link AuthorizableManagerPlugin}. 
 * This class was created as a way for the {@link Repository} to create a 
 * set of {@link AuthorizableManagerPlugin} objects to hand to each {@link Session}.
 */
public interface AuthorizableManagerPluginFactory {

	/**
	 * @return an instance of a specific {@link AuthorizableManagerPlugin}
	 */
	public AuthorizableManagerPlugin getAuthorizableManagerPlugin();

}
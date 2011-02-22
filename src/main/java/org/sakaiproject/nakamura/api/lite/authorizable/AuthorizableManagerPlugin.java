package org.sakaiproject.nakamura.api.lite.authorizable;

public interface AuthorizableManagerPlugin extends AuthorizableManager {
	/**
	 * Check if this {@link AuthorizableManagerPlugin} can handle an {@link Authorizable}
	 * with this id. This should be implemented as an efficient check to avoid more costly
	 * remote operations.
	 * @param authorizableId
	 * @return whether or not the this {@link AuthorizableManagerPlugin} should perform 
	 * 	operations on the {@link Authorizable}
	 */
	public boolean handles(String authorizableId);
}

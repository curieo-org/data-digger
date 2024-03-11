package org.curieo.rdf;

/**
 * View of namespace service which allows updates to the underlying store
 */
public interface NamespaceService extends ImmutableNamespaceService {

	/**
	 * put a new prefix-expansion mapping
	 *
	 * Not thread safe
	 */
	void put(String prefix, String fullForm);

}

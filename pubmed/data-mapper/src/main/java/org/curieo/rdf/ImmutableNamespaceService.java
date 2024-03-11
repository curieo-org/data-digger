package org.curieo.rdf;

import java.util.Set;

/**
 * Read only view of a namespace service
 */
public interface ImmutableNamespaceService {
	/**
	 * @return known prefixes
	 */
	Set<String> knownPrefixes();

	/**
	 * Get the standard prefix used for this uri
	 *
	 * @param uri URI
	 * @return prefix or {@code null} if none
	 */
	String getPrefix(String uri);

	/**
	 * This method will encode a uri
	 *
	 * @param uri URI
	 * @return encoded Uri
	 */
	String encodeUri(String uri);

	/**
	 * Get the full form for a prefix
	 *
	 * @param prefix prefix to lookup
	 * @return full form if known - null otherwise
	 */
	String getFullForm(String prefix);

	/**
	 * expand a uri to its full form from skos:Concept to https://http://www.w3.org/2004/02/skos/core#Concept
	 *
	 * @param uri URI
	 * @return full form (or the same if no prefix or : found)
	 */
	String decodeUri(String uri);
}

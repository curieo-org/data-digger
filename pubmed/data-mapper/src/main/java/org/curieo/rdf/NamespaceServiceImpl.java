package org.curieo.rdf;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.curieo.utils.Trie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceServiceImpl implements NamespaceService {
	public static final Logger LOGGER = LoggerFactory.getLogger(NamespaceServiceImpl.class);
	protected final Map<String, String> namespaces = new HashMap<>();
	private final Trie<String> nslookup = new Trie<>();

	/**
	 * Get the standard prefix used for this uri
	 * @param uri
	 * @return prefix or {@code null} if none
	 */
	@Override public String getPrefix(String uri) {
		// prefix upto last '/' or '#'
		int lastPos = Math.max(uri.lastIndexOf('#'), uri.lastIndexOf('/'));
		if (lastPos == -1) {
			return null;
		}
		String longPrefix = uri.substring(0,lastPos+1);
		for (Map.Entry<String, String> entry : namespaces.entrySet()) {
			String k = entry.getKey();
			String v = entry.getValue();
			if (v.equals(longPrefix)) {
				return k;
			}
		}
		return null;
	}

	/**
	 * This method will encode a uri
	 * @param uri
	 * @return encoded Uri
	 */
    @Override 
    public String encodeUri(String uri) {
		Trie<String>.TrieHit hit = nslookup.findLongestUpTo(uri, 0, uri.length());
		if (hit == null) {
			return null;
		}
		return String.format("%s:%s", hit.getValue(), uri.substring(hit.getLength()));
	}
		
	/**
	 * Get the full form for a prefix
	 * @param prefix
	 * @return full form if known - null otherwise
	 */
    @Override
	public String getFullForm(String prefix) {
		return namespaces.get(prefix);
	}

	@Override
	public void put(String prefix, String fullForm) {
		try {
			nslookup.put(fullForm, prefix);
			this.namespaces.put(prefix, fullForm);
		} catch (Exception e) {
			LOGGER.error(String.format("Failed to add namespace %s->%s", prefix, fullForm), e);
			throw e;
		}
	}

	@Override
	public String decodeUri(String s) {
		int colon = s.indexOf(':');
		if (colon >= 0 && namespaces.containsKey(s.substring(0, colon))) {
			return namespaces.get(s.substring(0, colon)) + s.substring(colon + 1);
		}
		return s;
	}

	@Override
	public Set<String> knownPrefixes() {
		return namespaces.keySet();
	}
}
